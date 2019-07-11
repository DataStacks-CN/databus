package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.weibo.dip.databus.source.FileSourceConfConstants.*;

/** Created by jianhong1 on 2019-07-05. */
public class FileSource extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSource.class);
  private static final String FILE_STATUS_PATTERN = "(\\S+) (\\S+) (\\S+)";
  private final AtomicBoolean fileReaderClosed = new AtomicBoolean(false);
  private LinkedBlockingQueue<File> fileQueue = new LinkedBlockingQueue<>();
  private ScheduledExecutorService fileScanner;
  private ExecutorService fileReader;
  private ScheduledExecutorService offsetRecorder;
  private ConcurrentHashMap<String, FileStatus> fileStatusMap = new ConcurrentHashMap<>();
  private String category;
  private int threadNumber;
  private String includePattern;
  private int scanInterval;
  private int flushInterval;

  private File targetDirectory;
  private File offsetFile;

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    String fileDirectory = conf.get(FILE_DIRECTORY);
    Preconditions.checkState(
        StringUtils.isNotEmpty(fileDirectory),
        String.format("%s %s must be specified", name, FILE_DIRECTORY));
    LOGGER.info("Property: {}={}", FILE_DIRECTORY, fileDirectory);

    category = conf.get(CATEGORY);
    Preconditions.checkState(
        StringUtils.isNotEmpty(category), String.format("%s %s must be specified", name, CATEGORY));
    LOGGER.info("Property: {}={}", CATEGORY, category);

    targetDirectory = new File(fileDirectory);
    if (targetDirectory.exists() && targetDirectory.isDirectory()) {
      // 若category offset文件不存在，则创建
      offsetFile = new File(String.format("%s/%s.offset", fileDirectory, category));

      if(offsetFile.createNewFile()){
        LOGGER.info("{} does not exist and was successfully created", offsetFile);
      } else {
        LOGGER.info("{} already exists", offsetFile);
      }
    } else {
      throw new Exception(fileDirectory + " is not exist or not directory!");
    }

    threadNumber = conf.getInteger(THREAD_NUMBER, DEFAULT_THREAD_NUMBER);
    LOGGER.info("Property: {}={}", THREAD_NUMBER, threadNumber);

    includePattern = conf.get(INCLUDE_PATTERN, DEFAULT_INCLUDE_PATTERN);
    LOGGER.info("Property: {}={}", INCLUDE_PATTERN, includePattern);

    scanInterval = conf.getInteger(SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL);
    LOGGER.info("Property: {}={}", SCAN_INTERVAL, scanInterval);

    flushInterval = conf.getInteger(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL);
    LOGGER.info("Property: {}={}", FLUSH_INTERVAL, flushInterval);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    // 加载file offset信息到map
    loadOffsetFile();

    // 定时扫描目录下的未读文件，放到队列中
    fileScanner =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("fileScanner-pool-%d").build());
    fileScanner.scheduleWithFixedDelay(
        new FileScannerRunnable(fileQueue), 0, scanInterval, TimeUnit.SECONDS);

    // 从队列中取出文件，把文件内容读到内存，一个文件由一个线程处理
    fileReader =
        Executors.newFixedThreadPool(
            threadNumber, new ThreadFactoryBuilder().setNameFormat("fileReader-pool-%d").build());
    for (int i = 0; i < threadNumber; i++) {
      fileReader.execute(new FileReaderRunnable(fileQueue));
    }

    // 把文件当前位置offset信息定时刷到磁盘
    offsetRecorder =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("offsetRecorder-pool-%d").build());
    offsetRecorder.scheduleWithFixedDelay(new OffsetRecorderRunnable(), 10, flushInterval, TimeUnit.SECONDS);

    LOGGER.info("{} started", name);
  }

  private void loadOffsetFile() {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(offsetFile)));

      String line;
      while ((line = in.readLine()) != null) {
        Matcher matcher = Pattern.compile(FILE_STATUS_PATTERN).matcher(line);

        if (matcher.find()) {
          FileStatus fileStatus = new FileStatus();
          fileStatus.setPath(matcher.group(1));
          fileStatus.setOffset(Long.valueOf(matcher.group(2)));
          fileStatus.setCompleted(Boolean.parseBoolean(matcher.group(3)));

          fileStatusMap.put(matcher.group(1), fileStatus);
          LOGGER.info("put '{}' into map", fileStatus);
        }
      }
      LOGGER.info("initial map size: {}", fileStatusMap.size());
    }catch (Exception e) {
      LOGGER.error("read offsetFile error: {}", ExceptionUtils.getStackTrace(e));
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOGGER.error(
            "close offsetFile BufferedReader error: {}", ExceptionUtils.getStackTrace(e));
      }
    }
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    fileScanner.shutdown();
    try {
      while (!fileScanner.awaitTermination(THREAD_POOL_AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        LOGGER.info("{} fileScanner await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} fileScanner await termination, but interrupted", name);
    }
    LOGGER.info("{} fileScanner stopped", name);

    fileReaderClosed.set(true);
    fileReader.shutdown();
    try {
      while (!fileReader.awaitTermination(THREAD_POOL_AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        LOGGER.info("{} fileReader await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} fileReader await termination, but interrupted", name);
    }
    LOGGER.info("{} fileReader stopped", name);

    offsetRecorder.shutdown();
    try {
      while (!offsetRecorder.awaitTermination(THREAD_POOL_AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        LOGGER.info("{} offsetRecorder await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} offsetRecorder await termination, but interrupted", name);
    }
    LOGGER.info("{} offsetRecorder stopped", name);

    LOGGER.info("{} stopped", name);
  }


  private class FileScannerRunnable implements Runnable {
    LinkedBlockingQueue<File> fileQueue;

    private FileScannerRunnable(LinkedBlockingQueue<File> fileQueue) {
      this.fileQueue = fileQueue;
    }

    @Override
    public void run() {
      try{
        HashSet<String> dirSet = new HashSet<>();
        for (File item : targetDirectory.listFiles()) {
          // 筛选符合正则的文件
          if (item.isFile() && item.getName().matches(includePattern)) {
            String filePath = item.getPath();
            dirSet.add(filePath);

            // 把未读的或者未读完的文件入队
            if (!fileStatusMap.containsKey(filePath)
                || (fileStatusMap.containsKey(filePath)
                    && !(fileStatusMap.get(filePath).isCompleted()))) {
              try {
                fileQueue.put(item);
                LOGGER.info("{} enqueue", item);
              } catch (InterruptedException e) { // ???能否移到外面
                LOGGER.error("{} enqueue, but interrupted", item);
              }
            }
          }
        }
        LOGGER.info("queue size: {}", fileQueue.size());

        // 删去map中过期的文件
        for (Map.Entry<String, FileStatus> entry : fileStatusMap.entrySet()) {

          if (!dirSet.contains(entry.getKey())) {
            fileStatusMap.remove(entry.getKey());
            LOGGER.info("remove expired file: {} from map", entry.getKey());
          }
        }
        LOGGER.info("map size: {}", fileStatusMap.size());

      } catch (Exception e) {
        LOGGER.error("read offsetFile error: {}", ExceptionUtils.getStackTrace(e));
      }
    }
  }

  private class FileReaderRunnable implements Runnable {
    LinkedBlockingQueue<File> fileQueue;

    private FileReaderRunnable(LinkedBlockingQueue<File> fileQueue) {
      this.fileQueue = fileQueue;
    }

    @Override
    public void run() {
      File item;
      BufferedReader in = null;
      try {
        while (!fileReaderClosed.get()) {
          item = fileQueue.poll(1, TimeUnit.SECONDS);

          if (item == null || !item.exists()) {
            continue;
          }

          LOGGER.info("{} dequeue", item);
          String filePath = item.getPath();
          FileStatus fileStatus;

          in = new BufferedReader(new InputStreamReader(new FileInputStream(item)));

          String line;
          long number = 0;
          long initOffset;

          // 处理未读文件
          if (!fileStatusMap.containsKey(filePath)) {
            fileStatus = new FileStatus();
            fileStatus.setPath(item.getPath());

            fileStatusMap.put(filePath, fileStatus);

            initOffset = -1;

            // 处理未读完文件
          } else {
            fileStatus = fileStatusMap.get(filePath);
            initOffset = fileStatus.getOffset();
          }

          while ((line = in.readLine()) != null && !fileReaderClosed.get()) {
            if (number > initOffset) {
              Message message = new Message(category, line);
              deliver(message);

              fileStatus.setOffset(number++);
            }
          }

          fileStatus.setCompleted(true);
          LOGGER.info("read {} completed", filePath);
        }

      } catch (Exception e) {
        LOGGER.error("read logFile error: {}", ExceptionUtils.getStackTrace(e));
      } finally {
        try {
          if (in != null) {
            in.close();
          }
        } catch (IOException e) {
          LOGGER.error("close BufferedReader error: {}", ExceptionUtils.getStackTrace(e));
        }
      }
    }
  }

  private class OffsetRecorderRunnable implements Runnable {

    @Override
    public void run() {
      BufferedWriter out = null;

      try {
        StringBuilder sb = new StringBuilder();
        for (FileStatus fileStatus : fileStatusMap.values()) {
          sb.append(fileStatus).append("\n");
        }

        if(sb.length() > 0){
          sb.deleteCharAt(sb.length() - 1);
        }

        out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(offsetFile)));

        String str = sb.toString();
        out.write(str);
        LOGGER.info("flush {} offsetRecords to disk", fileStatusMap.size());
      } catch (Exception e) {
        LOGGER.error("write offset to file error: {}", ExceptionUtils.getStackTrace(e));
      } finally {
        try {
          if (out != null) {
            out.close();
          }
        } catch (IOException e) {
          LOGGER.error("close BufferedWriter error: {}", ExceptionUtils.getStackTrace(e));
        }
      }
    }
  }
}

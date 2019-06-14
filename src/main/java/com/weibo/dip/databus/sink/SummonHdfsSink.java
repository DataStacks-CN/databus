package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.hadoop.compression.lzo.LzoCodec;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.IPUtil;
import com.weibo.dip.data.platform.commons.util.ProcessUtil;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianhong1 on 2018/5/23.
 */
public class SummonHdfsSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(SummonHdfsSink.class);

  // hdfs_base_dir/business/day_and_hour/filename
  private static final String FILE_PATH_PATTERN = "%s/%s/%s/%s";
  // business-localip-processid-threadid-timestamp.extension
  private static final String FILE_NAME_PATTERN = "%s-%s-%s-%s-%s.%s";

  private static final String DEFAULT_EXTENSION = "log";
  private static final String DEFLATE_EXTENSION = "deflate";
  private static final String GZIP_EXTENSION = "gz";
  private static final String BZIP2_EXTENSION = "bz2";
  private static final String LZO_EXTENSION = "lzo";
  private static final String LZ4_EXTENSION = "lz4";
  private static final String SNAPPY_EXTENSION = "snappy";

  private static final String FILE_COMPRESSION = "sink.hdfs.file.compression";
  private static final String HDFS_BASE_DIR = "sink.hdfs.base.dir";
  private static final String FILE_FLUSH_INTERVAL_MILLISECOND = "sink.hdfs.file.flush.interval.millisecond";
  private String fileCompression;
  private String hdfsBaseDir;
  private String fileFlushInterval;

  private final ConcurrentHashMap<String, HdfsFileStream> streams = new ConcurrentHashMap<>();
  private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyyMMddHH");
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock readLock = lock.readLock();
  private Lock writeLock = lock.writeLock();

  private org.apache.hadoop.conf.Configuration configuration;
  private FileSystem filesystem;
  private Flusher flusher;

  {
    try {
      configuration = new org.apache.hadoop.conf.Configuration();
      filesystem = FileSystem.get(configuration);
    } catch (IOException e) {
      throw new ExceptionInInitializerError("init hdfs filesystem error: " + e);
    }
  }

  /**
   * 2.一旦有数据过来，则执行写操作
   *
   * @param message 一条记录
   * @throws IOException 写hdfs文件异常
   */
  @Override
  public void process(Message message) throws Exception {
    String line = message.getData();
    if (StringUtils.isEmpty(line)) {
      return;
    }
    //获取一条消息的businessName和messageTimestamp
    HashMap<String, Object> map = GsonUtil.fromJson(line, HashMap.class);

    String businessName = map.get("business").toString();
    long messageTimestamp = ((Double) map.get("timestamp")).longValue();
    String dayHour = dateFormat.format(messageTimestamp);

    String businessDayHour = businessName + dayHour;

    readLock.lock();
    if (!streams.containsKey(businessDayHour)) {
      readLock.unlock();

      writeLock.lock();
      try {
        if (!streams.containsKey(businessDayHour)) {
          streams.put(businessDayHour, new HdfsFileStream(businessName, messageTimestamp));
        }
        readLock.lock();
      } finally {
        writeLock.unlock();
      }
    }

    try {
      streams.get(businessDayHour).write(line);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 0.根据配置信息设置HDFSSink
   *
   * @param conf 属性配置
   */
  @Override
  public void setConf(Configuration conf) {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    fileCompression = conf.get(FILE_COMPRESSION);
    LOGGER.info("Property: " + FILE_COMPRESSION + "=" + fileCompression);

    hdfsBaseDir = conf.get(HDFS_BASE_DIR);
    Preconditions.checkState(StringUtils.isNotEmpty(hdfsBaseDir),
        name + " " + HDFS_BASE_DIR + " must be specified");
    LOGGER.info("Property: " + HDFS_BASE_DIR + "=" + hdfsBaseDir);

    fileFlushInterval = conf.get(FILE_FLUSH_INTERVAL_MILLISECOND);
    Preconditions.checkState(StringUtils.isNumeric(fileFlushInterval),
        name + " " + FILE_FLUSH_INTERVAL_MILLISECOND + " must be numeric");
    LOGGER.info("Property: " + FILE_FLUSH_INTERVAL_MILLISECOND + "=" + fileFlushInterval);
  }

  /**
   * 1.每隔固定时间迭代一个hdfs文件
   */
  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    flusher = new Flusher();
    flusher.start();

    LOGGER.info("{} started", name);
  }

  /**
   * 3.停止写hdfs，释放资源
   */
  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    flusher.interrupt();

    try {
      flusher.join();
    } catch (InterruptedException e) {
      LOGGER.warn("{} flusher may be waitting for stop, but interrupted", name);
    }

    flush();
    LOGGER.info("{} stopped", name);
  }

  /**
   * 2.2 负责把一行记录写入HDFS
   * 一个线程对应一个HDFSWriter
   */
  private class HdfsWriter implements Closeable {
    private final SimpleDateFormat filePathDateFormat = new SimpleDateFormat("yyyy_MM_dd/HH");
    private final SimpleDateFormat fileNameDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    private String srcFilePath;
    private String dstFilePath;
    private BufferedWriter writer;

    public HdfsWriter(String business, long timestamp) throws IOException {
      String dayAndHour = filePathDateFormat.format(timestamp);
      String fileExtension = DEFAULT_EXTENSION;
      Class<?> codecClass = null;

      if (StringUtils.isNotEmpty(fileCompression)) {
        switch (fileCompression) {
          case DEFLATE_EXTENSION:
            fileExtension = DEFLATE_EXTENSION;
            codecClass = DeflateCodec.class;
            break;

          case GZIP_EXTENSION:
            fileExtension = GZIP_EXTENSION;
            codecClass = GzipCodec.class;
            break;

          case BZIP2_EXTENSION:
            fileExtension = BZIP2_EXTENSION;
            codecClass = BZip2Codec.class;
            break;

          case LZO_EXTENSION:
            fileExtension = LZO_EXTENSION;
            codecClass = LzoCodec.class;
            break;

          case LZ4_EXTENSION:
            fileExtension = LZ4_EXTENSION;
            codecClass = Lz4Codec.class;
            break;

          case SNAPPY_EXTENSION:
            fileExtension = SNAPPY_EXTENSION;
            codecClass = SnappyCodec.class;
            break;
        }
      }

      String hostname = null;
      try {
        hostname = IPUtil.getLocalhost();
      } catch (UnknownHostException e) {
        LOGGER.warn(ExceptionUtils.getFullStackTrace(e));
      }

      String fileName = String.format(FILE_NAME_PATTERN, business, hostname, ProcessUtil.getPid(),
          Thread.currentThread().getId(), fileNameDateFormat.format(System.currentTimeMillis()), fileExtension);
      String hideFileName = Symbols.FULL_STOP + fileName;

      srcFilePath = String
          .format(FILE_PATH_PATTERN, hdfsBaseDir, business, dayAndHour, hideFileName);
      dstFilePath = String.format(FILE_PATH_PATTERN, hdfsBaseDir, business, dayAndHour, fileName);

      if (Objects.isNull(codecClass)) {
        writer = new BufferedWriter(
            new OutputStreamWriter(filesystem.create(new Path(srcFilePath)), CharEncoding.UTF_8));
      } else {
        writer = new BufferedWriter(new OutputStreamWriter(
            ((CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration))
                .createOutputStream(filesystem.create(new Path(srcFilePath))), CharEncoding.UTF_8));
      }

      LOGGER.info("hdfs file {} created", srcFilePath);
    }

    public void write(String line) throws IOException {
      writer.write(line);
      if (line.charAt(line.length() - 1) != '\n') {
        writer.newLine();
      }
    }

    @Override
    public void close() throws IOException {
      if (writer != null) {
        writer.close();
        filesystem.rename(new Path(srcFilePath), new Path(dstFilePath));

        LOGGER.info("hdfs file {} closed", dstFilePath);
      }
    }
  }

  /**
   * 2.1 维护HDFSWriter
   * 一个 business+dayHour 对应一个HDFSFileStream
   */
  private class HdfsFileStream implements Closeable {
    //key:threadId, value:HDFSWriter
    private final ConcurrentHashMap<Long, HdfsWriter> writers = new ConcurrentHashMap<>();
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock rlock = readWriteLock.readLock();
    private Lock wlock = readWriteLock.writeLock();

    private String business;
    private long timestamp;

    public HdfsFileStream(String business, long timestamp) {
      this.business = business;
      this.timestamp = timestamp;
    }

    public void write(String line) throws Exception {
      long threadId = Thread.currentThread().getId();

      rlock.lock();
      if (!writers.containsKey(threadId)) {
        rlock.unlock();

        wlock.lock();
        try {
          if (!writers.containsKey(threadId)) {
            writers.put(threadId, new HdfsWriter(business, timestamp));
          }
          rlock.lock();
        } finally {
          wlock.unlock();
        }
      }

      try {
        writers.get(threadId).write(line);
      } finally {
        rlock.unlock();
      }

    }

    @Override
    public void close() throws IOException {
      if (!writers.isEmpty()) {
        for (HdfsWriter writer : writers.values()) {
          writer.close();
        }
      }
    }
  }

  private class Flusher extends Thread {

    @Override
    public void run() {
      while (!isInterrupted()) {
        try {
          Thread.sleep(Long.parseLong(fileFlushInterval));
        } catch (InterruptedException e) {
          LOGGER.info("flusher stopping...");
          break;
        }

        writeLock.lock();
        try {
          flush();
        } finally {
          writeLock.unlock();
        }
      }
    }
  }

  private void flush() {
    Iterator<Entry<String, HdfsFileStream>> iterator = streams.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, HdfsFileStream> entry = iterator.next();
      String businessDayHour = entry.getKey();
      HdfsFileStream stream = entry.getValue();

      iterator.remove();

      try {
        stream.close();
        LOGGER.info("businessDayHour: {} flusher close success", businessDayHour);
      } catch (IOException e) {
        LOGGER.error("businessDayHour: {} flusher close error\n{}",
            businessDayHour, ExceptionUtils.getFullStackTrace(e));
      }
    }
  }
}

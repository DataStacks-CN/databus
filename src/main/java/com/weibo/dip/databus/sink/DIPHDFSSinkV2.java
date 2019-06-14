package com.weibo.dip.databus.sink;

import com.hadoop.compression.lzo.LzopCodec;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.util.IPUtil;
import com.weibo.dip.data.platform.commons.util.ProcessUtil;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Created by yurun on 17/10/23. */
public class DIPHDFSSinkV2 extends Sink {

  private static final Logger LOGGER = LoggerFactory.getLogger(DIPHDFSSinkV2.class);

  private org.apache.hadoop.conf.Configuration configuration;
  private FileSystem filesystem;

  {
    try {
      configuration = new org.apache.hadoop.conf.Configuration();
      filesystem = FileSystem.get(configuration);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final String HDFS_BASE_DIR = "/user/hdfs/rawlog";

  /**
   * hdfs_base_dir/dataSet/year_and_day/hour.
   */
  private static final String FILE_PATH_PATTERN = "%s/%s/%s/%s";
  /**
   * dataSet-localip-processid-threadid-timestamp.extension
   */
  private static final String FILE_NAME_PATTERN = "%s-%s-%s-%s-%s.%s";

  private static final String DEFAULT_EXTENSION = "log";
  private static final String DEFLATE_EXTENSION = "deflate";
  private static final String GZIP_EXTENSION = "gz";
  private static final String BZIP2_EXTENSION = "bz2";
  private static final String LZO_EXTENSION = "lzo";
  private static final String LZ4_EXTENSION = "lz4";
  private static final String SNAPPY_EXTENSION = "snappy";

  private static final String COMPRESSION = "compression";

  private String compression;

  private class HDFSWriter implements Closeable {

    private final SimpleDateFormat filePathDateFormat = new SimpleDateFormat("yyyy_MM_dd/HH");
    private final SimpleDateFormat fileNameDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    private String srcFilePath;
    private String dstFilePath;

    private BufferedWriter writer;

    public HDFSWriter(String dataSet, long timestamp) throws Exception {
      String dayAndHour = filePathDateFormat.format(timestamp);

      String fileExtension = DEFAULT_EXTENSION;

      Class<?> codecClass = null;

      if (StringUtils.isNotEmpty(compression)) {
        switch (compression) {
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
            codecClass = LzopCodec.class;

            break;

          case LZ4_EXTENSION:
            fileExtension = LZ4_EXTENSION;
            codecClass = Lz4Codec.class;

            break;

          case SNAPPY_EXTENSION:
            fileExtension = SNAPPY_EXTENSION;
            codecClass = SnappyCodec.class;
            break;

          default:
            break;
        }
      }

      String fileName = String.format(FILE_NAME_PATTERN, dataSet, IPUtil.getLocalhost(),
              ProcessUtil.getPid(),
              Thread.currentThread().getId(),
              fileNameDateFormat.format(timestamp),
              fileExtension);

      String hideFileName = Symbols.FULL_STOP + fileName;

      srcFilePath =
          String.format(FILE_PATH_PATTERN, HDFS_BASE_DIR, dataSet, dayAndHour, hideFileName);
      dstFilePath = String.format(FILE_PATH_PATTERN, HDFS_BASE_DIR, dataSet, dayAndHour, fileName);

      if (Objects.isNull(codecClass)) {
        this.writer =
            new BufferedWriter(
                new OutputStreamWriter(
                    filesystem.create(new Path(srcFilePath)), CharEncoding.UTF_8));
      } else {
        this.writer =
            new BufferedWriter(
                new OutputStreamWriter(
                    ((CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration))
                        .createOutputStream(filesystem.create(new Path(srcFilePath))),
                    CharEncoding.UTF_8));
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
    public void close() {
      if (writer != null) {
        try {
          writer.close();
          filesystem.rename(new Path(srcFilePath), new Path(dstFilePath));
          LOGGER.info("hdfs file {} closed(renamed)", srcFilePath);
        } catch (Exception e) {
          LOGGER.error(
              "close or rename hdfs file : {} fail \n {}",
              srcFilePath,
              ExceptionUtils.getFullStackTrace(e));
        }
      }
    }
  }

  private class HDFSFileStream implements Closeable {

    private String dataSet;
    private long timestamp;

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Map<Long, HDFSWriter> writers;

    public HDFSFileStream(String dataset, long timestamp) {
      this.dataSet = dataset;
      this.timestamp = timestamp;

      writers = new HashMap<>();
    }

    public String getDataSet() {
      return dataSet;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void write(String line) throws Exception {
      if (StringUtils.isEmpty(line)) {
        return;
      }

      long threadId = Thread.currentThread().getId();

      lock.readLock().lock();

      if (!writers.containsKey(threadId)) {
        lock.readLock().unlock();

        lock.writeLock().lock();

        try {
          if (!writers.containsKey(threadId)) {
            writers.put(threadId, new HDFSWriter(dataSet, timestamp));
          }

          lock.readLock().lock();
        } finally {
          lock.writeLock().unlock();
        }
      }

      try {
        writers.get(threadId).write(line);
      } finally {
        lock.readLock().unlock();
      }
    }

    @Override
    public void close() {
      if (MapUtils.isNotEmpty(writers)) {
        for (HDFSWriter writer : writers.values()) {
          writer.close();
        }
      }
    }
  }

  private synchronized HDFSFileStream createHDFSFileStream(String dataset) throws Exception {
    return new HDFSFileStream(dataset, System.currentTimeMillis());
  }

  private Map<String, HDFSFileStream> streams = new HashMap<>();

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private long interval = 5 * 60 * 1000;

  private Flusher flusher;

  @Override
  public void setConf(Configuration conf) throws Exception {

    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + DIPHDFSSinkV2.class.getSimpleName();

    compression = conf.get(COMPRESSION);

    metric.config(this, conf);
  }

  private class Flusher extends Thread {

    @Override
    public void run() {
      while (!isInterrupted()) {
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          LOGGER.warn("{} flusher may be running, but interrupted", name);

          break;
        }

        lock.writeLock().lock();

        try {
          flush(System.currentTimeMillis());
        } finally {
          lock.writeLock().unlock();
        }
      }
    }
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    flusher = new Flusher();

    flusher.start();

    LOGGER.info("{} started", name);
  }

  private void flush(long now) {

    Iterator<Map.Entry<String, HDFSFileStream>> iterator = streams.entrySet().iterator();

    while (iterator.hasNext()) {
      HDFSFileStream stream = iterator.next().getValue();

      if (now - stream.getTimestamp() >= interval) {

        iterator.remove();

        stream.close();

        LOGGER.info(
            "dataSet: {}, timestamp: {}, flusher close success",
            stream.getDataSet(),
            stream.getTimestamp());
      }
    }
  }

  @Override
  public void process(Message message) throws Exception {
    String dataSet = message.getTopic();
    String line = message.getData();

    lock.readLock().lock();

    if (!streams.containsKey(dataSet)) {
      lock.readLock().unlock();

      lock.writeLock().lock();

      try {
        if (!streams.containsKey(dataSet)) {
          streams.put(dataSet, createHDFSFileStream(dataSet));
        }

        lock.readLock().lock();
      } finally {
        lock.writeLock().unlock();
      }
    }

    try {
      streams.get(dataSet).write(line);
      metric.collectSuccessCountAndRate(name, message, "Sink");
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void stop() {
    LOGGER.info("{} stoping...", name);

    flusher.interrupt();

    try {
      flusher.join();
    } catch (InterruptedException e) {
      LOGGER.warn("{} flusher may be waitting for stop, but interrupted", name);
    }

    flush(Long.MAX_VALUE);

    LOGGER.info("{} stoped", name);
  }
}

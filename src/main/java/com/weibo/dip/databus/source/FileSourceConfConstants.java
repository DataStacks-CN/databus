package com.weibo.dip.databus.source;

/**
 * Created by jianhong1 on 2019-07-05.
 */
public class FileSourceConfConstants {
  public static final String FILE_DIRECTORY = "source.file.directory";

  public static final String CATEGORY = "source.file.category";

  public static final String THREAD_NUMBER = "source.file.thread.number";
  public static final int DEFAULT_THREAD_NUMBER = 1;

  public static final String INCLUDE_PATTERN = "source.file.include.pattern";
  public static final String DEFAULT_INCLUDE_PATTERN = "^.*\\.log$";

  public static final String SCAN_INTERVAL = "source.file.scan.interval.second";
  public static final int DEFAULT_SCAN_INTERVAL = 30;

  public static final String FLUSH_INIT_DELAY = "source.file.flush.init.delay.second";
  public static final int DEFAULT_FLUSH_INIT_DELAY = 60;

  public static final String FLUSH_INTERVAL = "source.file.flush.interval.second";
  public static final int DEFAULT_FLUSH_INTERVAL = 60;

  public static final String RETENTION = "source.file.retention.second";
  public static final int DEFAULT_RETENTION = 86400;

  public static final String READ_ORDER = "source.file.read.order";
  public static final String DEFAULT_READ_ORDER = "asc";

  public static final String BUFFER_SIZE = "source.file.buffer.size";
  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final String LINE_BUFFER_SIZE = "source.file.line.buffer.size";
  public static final int DEFAULT_LINE_BUFFER_SIZE = 1024;

  public static final String DELETE_AFTER_READ = "source.file.delete.after.read";
  public static final boolean DEFAULT_DELETE_AFTER_READ = false;

  public static final int THREAD_POOL_AWAIT_TIMEOUT = 30;
}

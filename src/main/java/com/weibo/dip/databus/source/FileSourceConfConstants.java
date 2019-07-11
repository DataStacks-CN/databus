package com.weibo.dip.databus.source;

/**
 * Created by jianhong1 on 2019-07-05.
 */
public class FileSourceConfConstants {
  public static final String FILE_DIRECTORY = "source.file.directory";

  public static final String CATEGORY = "source.file.category";

  public static final String THREAD_NUMBER = "source.file.thread.number";
  public static final int DEFAULT_THREAD_NUMBER = 5;

  public static final String INCLUDE_PATTERN = "source.file.include.pattern";
  public static final String DEFAULT_INCLUDE_PATTERN = "^.*\\.log$";

  public static final String SCAN_INTERVAL = "source.file.scan.interval";
  public static final int DEFAULT_SCAN_INTERVAL = 30;

  public static final String FLUSH_INTERVAL = "source.file.flush.interval";
  public static final int DEFAULT_FLUSH_INTERVAL = 10;

  public static final int THREAD_POOL_AWAIT_TIMEOUT = 30;
}

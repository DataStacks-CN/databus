package com.weibo.dip.databus.sink;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class ScribeSinkConfContants {
  public static final String HOST = "sink.scribe.server.host";

  public static final String PORT = "sink.scribe.server.port";
  public static final int DEFAULT_PORT = 1466;

  public static final String BATCH_SIZE = "sink.scribe.batch.size";
  public static final int DEFAULT_BATCH_SIZE = 100;

  public static final String SEND_INTERVAL = "sink.scribe.max.send.interval";
  public static final int DEFAULT_SEND_INTERVAL = 1000;

  public static final String CAPACITY = "sink.scribe.buffer.capacity";
  public static final int DEFAULT_CAPACITY = 10000;

  public static final String THREAD_NUMBER = "sink.scribe.thread.number";
  public static final int DEFAULT_THREAD_NUMBER = 5;

  public static final String WORKER_SLEEP = "sink.scribe.worker.sleep";
  public static final int DEFAULT_WORKER_SLEEP = 30000;

  public static final String CHECK_INTERVAL = "sink.scribe.check.interval.second";
  public static final int DEFAULT_CHECK_INTERVAL = 60;
}

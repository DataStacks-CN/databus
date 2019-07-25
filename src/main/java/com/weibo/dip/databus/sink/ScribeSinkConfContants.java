package com.weibo.dip.databus.sink;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class ScribeSinkConfContants {
  public static final String HOST = "sink.scribe.server.host";

  public static final String PORT = "sink.scribe.server.port";
  public static final int DEFAULT_PORT = 1466;

  public static final String BATCH_SIZE = "sink.scribe.batch.size";
  public static final int DEFAULT_BATCH_SIZE = 20;

  public static final String SEND_INTERVAL = "sink.scribe.max.send.interval";
  public static final int DEFAULT_SEND_INTERVAL = 1000;
}

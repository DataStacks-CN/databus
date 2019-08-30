package com.weibo.dip.databus.sink;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class ScribeSinkConfContants {
  public static final String HOSTS = "sink.scribe.server.hosts";

  public static final String PORT = "sink.scribe.server.port";
  public static final int DEFAULT_PORT = 1466;

  public static final String BATCH_SIZE = "sink.scribe.batch.size";
  public static final int DEFAULT_BATCH_SIZE = 100;

  public static final String SEND_INTERVAL = "sink.scribe.max.send.interval";
  public static final int DEFAULT_SEND_INTERVAL = 1000;

  public static final String CAPACITY = "sink.scribe.buffer.capacity";
  public static final int DEFAULT_CAPACITY = 1000;

  public static final String THREAD_NUMBER = "sink.scribe.thread.number";
  public static final int DEFAULT_THREAD_NUMBER = 5;

  public static final String WORKER_SLEEP = "sink.scribe.worker.sleep";
  public static final long DEFAULT_WORKER_SLEEP = 600000;

  public static final String POLL_TIMEOUT = "sink.scribe.poll.timeout";
  public static final int DEFAULT_POLL_TIMEOUT = 1000;

  public static final String SOCKET_TIMEOUT = "sink.scribe.socket.timeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 60000;
}

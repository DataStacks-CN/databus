package com.weibo.dip.databus.sink;

/**
 * Created by jianhong1 on 2019-08-29.
 */
public class KafkaSinkV010ConfConstants {
  public static final String BROKERS = "sink.kafka.bootstrap.servers";

  public static final String KEY_SERIALIZER = "sink.kafka.key.serializer";
  public static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

  public static final String VALUE_SERIALIZER = "sink.kafka.value.serializer";
  public static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

  public static final String ACKS = "sink.kafka.acks";
  public static final String DEFAULT_ACKS = "1";

  public static final String RETRIES = "sink.kafka.retries";
  public static final int DEFAULT_RETRIES = 0;

  public static final String BATCH_SIZE = "sink.kafka.batch.size";
  public static final int DEFAULT_BATCH_SIZE = 16384;

  public static final String LINGER_MS = "sink.kafka.linger.ms";
  public static final long DEFAULT_LINGER_MS = 0;

  public static final String BUFFER_MEMORY = "sink.kafka.buffer.memory";
  public static final long DEFAULT_BUFFER_MEMORY = 33554432;

  public static final String COMPRESSION = "sink.kafka.compression.type";
  public static final String DEFAULT_COMPRESSION = "none";

  public static final String THREAD_NUMBER = "sink.kafka.thread.number";
  public static final int DEFAULT_THREAD_NUMBER = 6;

  public static final String CAPACITY = "sink.kafka.queue.capacity";
  public static final int DEFAULT_CAPACITY = 10000;

  public static final String POLL_TIMEOUT = "sink.kafka.poll.timeout";
  public static final int DEFAULT_POLL_TIMEOUT = 1000;
}

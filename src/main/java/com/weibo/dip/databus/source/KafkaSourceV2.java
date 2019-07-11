package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Created by yurun on 17/8/9. */
public class KafkaSourceV2 extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceV2.class);

  private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final String GROUP_ID = "group.id";
  private static final String TOPIC_AND_THREADS = "topic.and.threads";
  private static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  private static final long STOP_SLEEP = 3000L;

  private String zookeeperAddress;
  private String groupId;
  private String[] topicAndThreads;
  private String autoOffsetReset;
  private ConsumerConnector kafkaConnector;

  private ExecutorService streamers = Executors.newCachedThreadPool();

  @Override
  public void setConf(Configuration conf) {
    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + KafkaSourceV2.class.getSimpleName();

    zookeeperAddress = conf.get(ZOOKEEPER_CONNECT);
    Preconditions.checkState(
        StringUtils.isNotEmpty(zookeeperAddress),
        name + " " + ZOOKEEPER_CONNECT + " must be specified");

    groupId = conf.get(GROUP_ID);
    Preconditions.checkState(
        StringUtils.isNotEmpty(groupId), name + " " + GROUP_ID + " must be specified");

    topicAndThreads = conf.get(TOPIC_AND_THREADS).split(Constants.COMMA);
    Preconditions.checkState(
        ArrayUtils.isNotEmpty(topicAndThreads),
        name + " " + TOPIC_AND_THREADS + " must be specified");

    autoOffsetReset = conf.get(AUTO_OFFSET_RESET);

  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    Properties properties = new Properties();
    properties.put(GROUP_ID, groupId);

    properties.put(ZOOKEEPER_CONNECT, zookeeperAddress);

    if (StringUtils.isNotEmpty(autoOffsetReset)) {
      properties.put(AUTO_OFFSET_RESET, autoOffsetReset);
    }

    ConsumerConfig config = new ConsumerConfig(properties);
    kafkaConnector = Consumer.createJavaConsumerConnector(config);

    Map<String, Integer> topicThreads = new HashMap<>();

    for (String topicAndThread : topicAndThreads) {
      String[] words = topicAndThread.split(Constants.COLON);

      if (ArrayUtils.isNotEmpty(words) && words.length == 2) {
        String topic = words[0];
        int thread = Integer.parseInt(words[1]);

        topicThreads.put(topic, thread);
      } else {
        LOGGER.error("{} wrong format, the format should be 'TOPIC:THREAD'", topicAndThread);
      }
    }

    Map<String, List<KafkaStream<byte[], byte[]>>> topicStreams =
        kafkaConnector.createMessageStreams(topicThreads);

    for (Entry<String, Integer> topicThread : topicThreads.entrySet()) {
      List<KafkaStream<byte[], byte[]>> streams = topicStreams.get(topicThread.getKey());
      for (KafkaStream<byte[], byte[]> stream : streams) {
        streamers.execute(new Streamer(stream));
      }
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    kafkaConnector.shutdown();

    streamers.shutdown();

    try {
      while (!streamers.awaitTermination(STOP_SLEEP, TimeUnit.MILLISECONDS)) {
        LOGGER.info("{} streamers await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} streamers await termination, but interrupted", name);
    }

    LOGGER.info("{} stopped", name);
  }

  private class Streamer implements Runnable {

    private KafkaStream<byte[], byte[]> stream;

    private Streamer(KafkaStream<byte[], byte[]> stream) {
      this.stream = stream;
    }

    @Override
    public void run() {
      LOGGER.info(name + " streamer " + Thread.currentThread().getName() + " started");
      try {
        for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
          String topic = messageAndMetadata.topic();
          String data;
          try {
            data = new String(messageAndMetadata.message(), CharEncoding.UTF_8);
          } catch (UnsupportedEncodingException e) {
            LOGGER.warn("unsupport encode " + CharEncoding.UTF_8);
            continue;
          }

          Message message = new Message(topic, data);
          deliver(message);
        }
      } catch (Exception e) {
        String subject = String.format("%s %s stopped", name, Thread.currentThread().getName());
        String content = String.format("%s \n%s", subject, ExceptionUtils.getStackTrace(e));
        LOGGER.error(content);

      }
      LOGGER.info(name + " streamer " + Thread.currentThread().getName() + " stopped");
    }
  }
}

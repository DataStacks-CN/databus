package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.weibo.dip.databus.source.SourceConfConstants.*;

/**
 * Created by jianhong1 on 2018/6/27.
 */
public class KafkaSourceV010 extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceV010.class);
  private static final String TOPIC_AND_THREADS = "source.kafka.topic.and.threads";
  private static final String PREFIX = "source.kafka.";
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private ExecutorService threadPool = Executors.newCachedThreadPool();
  private Properties props = new Properties();
  private String topicAndThreads;

  @Override
  public void setConf(Configuration conf) {
    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    //自动获取pipeline文件中关于Kafka消费者的配置
    Set<String> keys = conf.getKeys();
    for (String key: keys){
      if(key.startsWith(PREFIX)){
        String consumeKey = key.substring(PREFIX.length());
        props.put(consumeKey, conf.get(key));
      }
    }

    for (String propertyName: props.stringPropertyNames()){
      LOGGER.info("Property: {}={}", propertyName, props.getProperty(propertyName));
    }

    //自定义的配置
    topicAndThreads = conf.get(TOPIC_AND_THREADS);
    Preconditions.checkState(StringUtils.isNotEmpty(topicAndThreads),
        name + " " + TOPIC_AND_THREADS + " must be specified");
    LOGGER.info("Property: {}={}", TOPIC_AND_THREADS, topicAndThreads);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    Map<String, Integer> topicThreads = new HashMap<>();
    for (String topicAndThread : topicAndThreads.split(Constants.COMMA)) {
      String[] strs = topicAndThread.split(Constants.COLON);

      if (ArrayUtils.isNotEmpty(strs) && strs.length == 2) {
        String topic = strs[0];
        int threadNumber = Integer.parseInt(strs[1]);

        topicThreads.put(topic, threadNumber);
      } else {
        throw new IllegalArgumentException(name + " " + topicAndThreads
            + " wrong format, the format should be 'topic1:number1,topic2:number2'");
      }
    }

    for (Entry<String, Integer> topicThread : topicThreads.entrySet()) {
      String topic = topicThread.getKey();
      Integer thread = topicThread.getValue();

      for (int i = 0; i < thread; i++) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        threadPool.execute(new KafkaConsumerRunner(consumer, topic));
      }
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    closed.set(true);

    // Disable new tasks from being submitted
    threadPool.shutdown();

    boolean flag = true;
    try {
      // Wait a while for existing tasks to terminate
      if(!threadPool.awaitTermination(AWAIT_TIMEOUT_SECOND, TimeUnit.SECONDS)){
        LOGGER.warn("{} threadPool can not terminate in {}s, Cancel currently executing tasks", name, AWAIT_TIMEOUT_SECOND);
        threadPool.shutdownNow();
        // Wait a while for tasks to respond to being cancelled
        if(!threadPool.awaitTermination(AWAIT_TIMEOUT_SECOND, TimeUnit.SECONDS)) {
          LOGGER.error("{} threadPool did not terminate", name);
          flag = false;
        }
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} threadPool await termination but interrupted, cancel currently executing tasks again", name);
      threadPool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    if(flag) {
      LOGGER.info("{} stopped", name);
    } else {
      LOGGER.error("{} stop failed", name);
    }
  }

  private class KafkaConsumerRunner implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    private KafkaConsumerRunner(KafkaConsumer<String, String> consumer, String topic) {
      this.consumer = consumer;
      this.topic = topic;
    }

    @Override
    public void run() {
      LOGGER.info(name + " consumer " + Thread.currentThread().getName() + " started");

      try {
        consumer.subscribe(Collections.singletonList(topic));
        while (!closed.get()) {
          ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MS);
          for (ConsumerRecord<String, String> record : records) {
            deliver(new Message(topic, record.value()));
          }
        }
      } catch (WakeupException e) {
        if (!closed.get()) {
          throw e;
        }
      } finally {
        consumer.close();
      }

      LOGGER.info(name + " consumer " + Thread.currentThread().getName() + " stopped");
    }
  }
}

package com.weibo.dip.databus.sink;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.weibo.dip.databus.sink.KafkaSinkV010ConfConstants.*;
import static com.weibo.dip.databus.source.FileSourceConfConstants.THREAD_POOL_AWAIT_TIMEOUT;

/**
 * Created by jianhong1 on 2018/9/25.
 */
public class KafkaSinkV010 extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkV010.class);
  private String brokers;
  private String keySerializer;
  private String valueSerializer;
  private String acks;
  private int retries;
  private int batchSize;
  private long lingerMs;
  private long bufferMemory;
  private String compression;
  private int threadNumber;
  private int capacity;
  private int pollTimeout;

  private final AtomicBoolean senderClosed = new AtomicBoolean(false);
  private LinkedBlockingQueue<Message> recordQueue;
  private ExecutorService sender;

  private KafkaProducer<String, String> producer;

  @Override
  public void process(Message message) throws Exception {
    if (Strings.isNullOrEmpty(message.getData())) {
      return;
    }
    recordQueue.put(message);
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    brokers = conf.get(BROKERS);
    Preconditions.checkState(StringUtils.isNotEmpty(brokers),
        name + " " + BROKERS + " must be specified");
    LOGGER.info("Property: {}={}", BROKERS, brokers);

    keySerializer = conf.getString(KEY_SERIALIZER, DEFAULT_KEY_SERIALIZER);
    LOGGER.info("Property: {}={}", KEY_SERIALIZER, keySerializer);

    valueSerializer = conf.getString(VALUE_SERIALIZER, DEFAULT_VALUE_SERIALIZER);
    LOGGER.info("Property: {}={}", VALUE_SERIALIZER, valueSerializer);

    acks = conf.getString(ACKS, DEFAULT_ACKS);
    LOGGER.info("properties: {}={}", ACKS, acks);

    retries = conf.getInteger(RETRIES, DEFAULT_RETRIES);
    LOGGER.info("properties: {}={}", RETRIES, retries);

    batchSize = conf.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    LOGGER.info("properties: {}={}", BATCH_SIZE, batchSize);

    lingerMs = conf.getLong(LINGER_MS, DEFAULT_LINGER_MS);
    LOGGER.info("properties: {}={}", LINGER_MS, lingerMs);

    bufferMemory = conf.getLong(BUFFER_MEMORY, DEFAULT_BUFFER_MEMORY);
    LOGGER.info("Property: {}={}", BUFFER_MEMORY, bufferMemory);

    compression = conf.getString(COMPRESSION, DEFAULT_COMPRESSION);
    LOGGER.info("properties: {}={}", COMPRESSION, compression);

    threadNumber = conf.getInteger(THREAD_NUMBER, DEFAULT_THREAD_NUMBER);
    LOGGER.info("Property: {}={}", THREAD_NUMBER, threadNumber);

    capacity = conf.getInteger(CAPACITY, DEFAULT_CAPACITY);
    LOGGER.info("Property: {}={}", CAPACITY, capacity);

    pollTimeout = conf.getInteger(POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT);
    LOGGER.info("Property: {}={}", POLL_TIMEOUT, pollTimeout);

    metric.gauge(MetricRegistry.name(name, "recordQueue", "size"), () -> recordQueue.size());
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    recordQueue = new LinkedBlockingQueue<>(capacity);

    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", keySerializer);
    props.put("value.serializer", valueSerializer);
    props.put("acks", acks);
    props.put("retries", retries);
    props.put("batch.size", batchSize);
    props.put("linger.ms", lingerMs);
    props.put("buffer.memory", bufferMemory);
    props.put("compression.type", compression);
    producer = new KafkaProducer<>(props);

    sender =
        Executors.newFixedThreadPool(
            threadNumber, new ThreadFactoryBuilder().setNameFormat("sender-pool-%d").build());
    for (int i = 0; i < threadNumber; i++) {
      sender.execute(new NetworkSender(producer));
    }

    LOGGER.info("{} started", name);
  }

  private class NetworkSender implements Runnable{
    KafkaProducer<String, String> producer;

    public NetworkSender(KafkaProducer<String, String> producer){
      this.producer = producer;
    }

    @Override
    public void run() {
      // 当标志位为true且队列为空时退出循环
      while (!senderClosed.get() || recordQueue.size() > 0) {
        try {
          Message message = recordQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
          if(message == null){
            continue;
          }

          producer.send(new ProducerRecord<>(message.getTopic(), message.getData()));
        } catch (InterruptedException e) {
          LOGGER.warn("{}", ExceptionUtils.getStackTrace(e));
        } catch (Exception e) {
          LOGGER.warn("send entry error: {}", ExceptionUtils.getStackTrace(e));
        }
      }
    }
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    senderClosed.set(true);
    sender.shutdown();
    try {
      while (!sender.awaitTermination(THREAD_POOL_AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        LOGGER.info("{} sender await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} sender await termination, but interrupted", name);
    }
    LOGGER.info("{} sender stopped", name);

    metric.remove(MetricRegistry.name(name, "recordQueue", "size"));

    LOGGER.info("{} stopped", name);
  }
}

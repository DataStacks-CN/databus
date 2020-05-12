package com.weibo.dip.databus.persist;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Metric;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by jianhong1 on 2020-05-12.
 */
public class KafkaPersist implements Metric.Persist{
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPersist.class);
  private static final String PREFIX = "persist.kafka.";
  private static final String TOPIC = "persist.kafka.topic";

  private Properties props = new Properties();
  private KafkaProducer<String, String> producer;
  private String persistTopic;
  private String hostname;

  @Override
  public void persist(List<Metric.Counter> counters) {
    if (CollectionUtils.isEmpty(counters)) {
      return;
    }

    for (Metric.Counter counter : counters) {
      String className = counter.getName();
      String category = counter.getTopic();
      long delta = counter.getDelta();
      String line = String.format("%s\t%s\t%s\t%d", hostname, className, category, delta);
      producer.send(new ProducerRecord<>(persistTopic, line));
    }
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    Set<String> keys = conf.getKeys();
    for (String key: keys){
      if(key.startsWith(PREFIX)){
        String consumeKey = key.substring(PREFIX.length());
        props.put(consumeKey, conf.get(key));
      }
    }

    persistTopic = conf.get(TOPIC);

    for (String propertyName: props.stringPropertyNames()){
      LOGGER.info("KafkaPersist Property: {}={}", propertyName, props.getProperty(propertyName));
    }

    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOGGER.warn("{}", ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public void start() {
    producer = new KafkaProducer<>(props);

    LOGGER.info("KafkaPersist started");
  }

  @Override
  public void stop() {
    producer.close();

    LOGGER.info("KafkaPersist stopped");
  }
}

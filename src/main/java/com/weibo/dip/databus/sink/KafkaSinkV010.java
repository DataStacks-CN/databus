package com.weibo.dip.databus.sink;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;

/**
 * Created by jianhong1 on 2018/9/25.
 */
public class KafkaSinkV010 extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkV010.class);
  private static final String PREFIX = "sink.kafka.";

  private Properties props = new Properties();
  private KafkaProducer<String, String> producer;

  @Override
  public void process(Message message) throws Exception {
    producer.send(new ProducerRecord<>(message.getTopic(), message.getData()));
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    name =
        conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    //自动获取pipeline文件中关于Kafka生产者的配置
    Set<String> keys = conf.getKeys();
    for (String key: keys){
      if(key.startsWith(PREFIX)){
        String produceKey = key.substring(PREFIX.length());
        props.put(produceKey, conf.get(key));
      }
    }

    for (String propertyName: props.stringPropertyNames()){
      LOGGER.info("Property: {}={}", propertyName, props.getProperty(propertyName));
    }
  }

  @Override
  public void start() {
    producer = new KafkaProducer<>(props);

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    producer.close();

    LOGGER.info("{} stopped", name);
  }
}

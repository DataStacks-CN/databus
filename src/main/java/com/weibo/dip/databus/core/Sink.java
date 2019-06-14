package com.weibo.dip.databus.core;

import com.weibo.dip.databus.store.Store;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/8/9.
 */
public abstract class Sink implements Configurable, Lifecycle, Deliverable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Sink.class);

  protected String name;

  protected Store store;

  protected Metric metric = Metric.getInstance();

  public Sink() {

  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public void link(Store store) {
    this.store = store;
  }

  public abstract void process(Message message) throws Exception;

  @Override
  public void deliver(Message message) {
    try {
      process(message);

      metric.increment(name, message.getTopic(), 1L);
    } catch (Exception e) {
      metric.collectSinkFailCount(name,message);
      //把写入失败的message暂存起来
      store.store(message);
      LOGGER.error("{} deliver message error: {}", name, ExceptionUtils.getFullStackTrace(e));

      if (Alarm.alarmEnable()) {
        Alarm.sendAlarm();
      }
    }
  }
}

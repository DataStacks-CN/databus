package com.weibo.dip.databus.sink;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/8/30.
 */
public class PrintSink extends Sink {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrintSink.class);

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void process(Message message) {
    LOGGER.info("topic: {}, data: {}", message.getTopic(), message.getData());
  }

}

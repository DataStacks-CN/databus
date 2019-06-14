package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by jianhong1 on 2018/8/2.
 */
public class FileSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSink.class);
  private static final String FILE_PATH = "sink.file.path";

  private BufferedWriter writer;

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    String filePath = conf.get(FILE_PATH);
    Preconditions.checkNotNull(filePath, name + " " + FILE_PATH + " must be specified");
    LOGGER.info("Property: {}={}", FILE_PATH, filePath);

    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath)));
  }

  //有抛出异常比较好，把writer初始化放到这个方法，如果sink有异常抛出，则不启动这个Pipeline
  @Override
  public void start() {
    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.error("close writer error \n{}", ExceptionUtils.getFullStackTrace(e));
      }
    }

    LOGGER.info("{} stopped", name);
  }

  @Override
  public void process(Message message) {
    try {
      writer.write(message.getData());
      writer.newLine();
    } catch (IOException e) {
      LOGGER.error("write message to file error, {}", ExceptionUtils.getFullStackTrace(e));
    }
  }
}

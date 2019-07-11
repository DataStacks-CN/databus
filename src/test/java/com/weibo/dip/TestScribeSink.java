package com.weibo.dip;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.source.scribe.LogEntry;
import org.apache.flume.source.scribe.ResultCode;
import org.apache.flume.source.scribe.Scribe;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.weibo.dip.databus.sink.ScribeSinkConfContants.*;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class TestScribeSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestScribeSink.class);
  private LinkedBlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
  private ExecutorService sender;
  private String host;
  private int port;
  private int threadNumber;
  private int batchSize;

  private Scribe.Client client;


  @Override
  public void process(Message message) throws Exception {
    messageQueue.put(message);
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    host = conf.get(HOST);
    Preconditions.checkState(StringUtils.isNotEmpty(host),
        String.format("%s %s must be specified", name, HOST));
    LOGGER.info("Property: {}={}", HOST, host);

    port = conf.getInteger(PORT, DEFAULT_PORT);
    LOGGER.info("Property: {}={}", PORT, port);

    threadNumber = conf.getInteger(THREAD_NUMBER, DEFAULT_THREAD_NUMBER);
    LOGGER.info("Property: {}={}", THREAD_NUMBER, threadNumber);

    batchSize = conf.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    LOGGER.info("Property: {}={}", BATCH_SIZE, batchSize);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    sender = Executors.newFixedThreadPool(threadNumber, new ThreadFactoryBuilder().setNameFormat("sender-pool-%d").build());
    for (int i = 0; i < threadNumber; i++) {
      sender.execute(new SenderRunnable());
    }

    LOGGER.info("{} started", name);
  }

  class SenderRunnable implements Runnable{

    @Override
    public void run() {
      TTransport transport = null;
      try {
        transport = new TFramedTransport(new TSocket(new Socket(host, port)));
        client = new Scribe.Client(new TBinaryProtocol(transport));

        int number = 0;
        List<LogEntry> entrys = new ArrayList<>();
        while (true){
          Message message = messageQueue.poll(10, TimeUnit.SECONDS);
          entrys.add(new LogEntry(message.getTopic(), message.getData()));

          if(number ++ > batchSize){
            ResultCode result = client.Log(entrys);
            if (result.getValue() == 0) {
              number = 0;
              entrys.clear();
            }
          }
        }
      } catch (TTransportException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (TException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void stop() {

  }
}

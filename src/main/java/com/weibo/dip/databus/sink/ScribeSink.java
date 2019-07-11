package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flume.source.scribe.LogEntry;
import org.apache.flume.source.scribe.ResultCode;
import org.apache.flume.source.scribe.Scribe;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.weibo.dip.databus.sink.ScribeSinkConfContants.*;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class ScribeSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScribeSink.class);
  private String host;
  private int port;
  private int batchSize;

  private ConcurrentHashMap<Long, NetworkSender> senders = new ConcurrentHashMap<>();

  @Override
  public void process(Message message) throws Exception {
    if(StringUtils.isEmpty(message.getData())){
      return;
    }
    LogEntry entry = new LogEntry(message.getTopic(), message.getData());

    long threadId = Thread.currentThread().getId();

    if(!senders.containsKey(threadId)){
      senders.put(threadId, new NetworkSender());
    }

    senders.get(threadId).send(entry);
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

    batchSize = conf.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    LOGGER.info("Property: {}={}", BATCH_SIZE, batchSize);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    LOGGER.info("{} started", name);
  }


  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    for (Map.Entry<Long, NetworkSender> sender : senders.entrySet()){
      sender.getValue().close();
    }

    LOGGER.info("{} stopped", name);
  }

  public class NetworkSender implements Closeable {
    Scribe.Client client = null;
    TTransport transport = null;

    public NetworkSender(){

      try {
        transport = new TFramedTransport(new TSocket(new Socket(host, port)));
        client = new Scribe.Client(new TBinaryProtocol(transport));

        LOGGER.info("initial Scribe.Client completed");
      } catch (Exception e) {
        if (transport != null) {
          transport.close();
        }

        LOGGER.info("initial Scribe.Client fail: {}", ExceptionUtils.getStackTrace(e));
      }
    }

    int number = 0;
    List<LogEntry> entrys = new ArrayList<>();
    public void send(LogEntry entry){  //异常处理？？？
      entrys.add(entry);

      try{
        if(number ++ > batchSize){
          ResultCode result = client.Log(entrys);
          if (result.getValue() == 0) {
            LOGGER.info("send {} records", number);
            number = 0;
            entrys.clear();
          }
        }
      }catch (Exception e){
        LOGGER.info("{}", ExceptionUtils.getStackTrace(e));
      }
    }

    @Override
    public void close() {
      try {
        while (client.recv_Log().getValue() == 0){
          if(transport != null){
            transport.close();
          }
        }
      } catch (Exception e) {
        LOGGER.info("close socket error: {}", ExceptionUtils.getStackTrace(e));
      }
    }
  }
}

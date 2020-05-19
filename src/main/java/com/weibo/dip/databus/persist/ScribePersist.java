package com.weibo.dip.databus.persist;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Metric;
import com.weibo.dip.databus.utils.LocalHost;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flume.source.scribe.LogEntry;
import org.apache.flume.source.scribe.Scribe;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by jianhong1 on 2020-05-19.
 */
public class ScribePersist implements Metric.Persist {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScribePersist.class);
  /**
   * scribe server地址，格式：host1:port1,host2:port2
   */
  private static final String SERVER_ADDRESS = "persist.scribe.server.address";
  /**
   * 服务端接收的category
   */
  private static final String CATEGORY = "persist.scribe.category";

  private List<HostPort> hostPorts = new ArrayList<>();
  private String serverAddress;
  private String metricCategory;

  @Override
  public void persist(List<Metric.Counter> counters) {
    if (CollectionUtils.isEmpty(counters)) {
      return;
    }

    List<LogEntry> logEntryList = new ArrayList();
    for (Metric.Counter counter : counters) {
      String className = counter.getName();
      String category = counter.getTopic();
      long delta = counter.getDelta();
      // 指标写入格式规范?
      String line = String.format("%s\t%s\t%s\t%d", LocalHost.getHostname(), className, category, delta);
      logEntryList.add(new LogEntry(metricCategory, line));
    }

    HostPort hostPort = hostPorts.get(new Random().nextInt(hostPorts.size()));
    TTransport transport = null;
    try {
      transport = new TFramedTransport(new TSocket(new Socket(hostPort.host, hostPort.port)));
      Scribe.Client client = new Scribe.Client(new TBinaryProtocol(transport));

      //写入失败的场景如何处理？
      client.Log(logEntryList);
      LOGGER.info("{} persist num: {}", hostPort, logEntryList.size());
    } catch (Exception e) {
      LOGGER.error("{}", ExceptionUtils.getStackTrace(e));
    } finally {
      if (transport != null) {
        transport.close();
        LOGGER.info("{} closed", hostPort);
      }
    }
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    serverAddress = conf.get(SERVER_ADDRESS);
    Preconditions.checkState(
        StringUtils.isNotEmpty(serverAddress), String.format("%s must be specified", SERVER_ADDRESS));
    LOGGER.info("Property: {}={}", SERVER_ADDRESS, serverAddress);

    metricCategory = conf.get(CATEGORY);
    Preconditions.checkState(
        StringUtils.isNotEmpty(metricCategory), String.format("%s must be specified", CATEGORY));
    LOGGER.info("Property: {}={}", CATEGORY, metricCategory);

    String[] servers = serverAddress.split(Constants.COMMA);
    for (String server: servers){
      String[] hostPort = server.split(Constants.COLON);
      String host = hostPort[0];
      int port = Integer.parseInt(hostPort[1]);
      hostPorts.add(new HostPort(host, port));
    }
  }

  @Override
  public void start() {
    LOGGER.info("ScribePersist started");
  }

  @Override
  public void stop() {
    LOGGER.info("ScribePersist started");
  }

  static class HostPort{
    private String host;
    private int port;

    HostPort(String host, int port){
      this.host = host;
      this.port = port;
    }

    @Override
    public String toString() {
      return "HostPort{" +
          "host='" + host + '\'' +
          ", port=" + port +
          '}';
    }
  }
}

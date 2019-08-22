package com.weibo.dip.databus.sink;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.weibo.dip.databus.sink.ScribeSinkConfContants.*;
import static com.weibo.dip.databus.source.FileSourceConfConstants.THREAD_POOL_AWAIT_TIMEOUT;

/** Created by jianhong1 on 2019-07-10. */
public class ScribeSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScribeSink.class);
  private final AtomicBoolean senderClosed = new AtomicBoolean(false);
  private ExecutorService sender;
  private LinkedBlockingQueue<LogEntry> recordQueue;
  private String host;
  private int port;
  private int batchSize;
  private int sendInterval;
  private int capacity;
  private int threadNumber;
  private int workerSleep;
  private int pollTimeout;
  private int socketTimeout;
  private String prefix;

  @Override
  public void process(Message message) throws Exception {
    if (StringUtils.isEmpty(message.getData())) {
      return;
    }
    LogEntry entry = new LogEntry(message.getTopic(), prefix + message.getData());

    recordQueue.put(entry);
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    host = conf.get(HOST);
    Preconditions.checkState(
        StringUtils.isNotEmpty(host), String.format("%s %s must be specified", name, HOST));
    LOGGER.info("Property: {}={}", HOST, host);

    port = conf.getInteger(PORT, DEFAULT_PORT);
    LOGGER.info("Property: {}={}", PORT, port);

    batchSize = conf.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    LOGGER.info("Property: {}={}", BATCH_SIZE, batchSize);

    sendInterval = conf.getInteger(SEND_INTERVAL, DEFAULT_SEND_INTERVAL);
    LOGGER.info("Property: {}={}", SEND_INTERVAL, sendInterval);

    capacity = conf.getInteger(CAPACITY, DEFAULT_CAPACITY);
    LOGGER.info("Property: {}={}", CAPACITY, capacity);

    threadNumber = conf.getInteger(THREAD_NUMBER, DEFAULT_THREAD_NUMBER);
    LOGGER.info("Property: {}={}", THREAD_NUMBER, threadNumber);

    workerSleep = conf.getInteger(WORKER_SLEEP, DEFAULT_WORKER_SLEEP);
    LOGGER.info("Property: {}={}", WORKER_SLEEP, workerSleep);

    pollTimeout = conf.getInteger(POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT);
    LOGGER.info("Property: {}={}", POLL_TIMEOUT, pollTimeout);

    socketTimeout = conf.getInteger(SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
    LOGGER.info("Property: {}={}", SOCKET_TIMEOUT, socketTimeout);

    metric.gauge(MetricRegistry.name(name, "recordQueue", "size"), () -> recordQueue.size());

    String ip = null;
    try {
      ip = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOGGER.warn("{}", ExceptionUtils.getStackTrace(e));
    }
    prefix = "_accesskey=&_ip=" + ip + "&_port=&_an=&_data=";
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    recordQueue = new LinkedBlockingQueue<>(capacity);

    sender =
        Executors.newFixedThreadPool(
            threadNumber, new ThreadFactoryBuilder().setNameFormat("sender-pool-%d").build());
    for (int i = 0; i < threadNumber; i++) {
      sender.execute(new NetworkSender());
    }

    LOGGER.info("{} started", name);
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

  public class NetworkSender implements Runnable {
    Scribe.Client client = null;
    TTransport transport = null;
    List<LogEntry> entries = new ArrayList<>();
    long lastTime = System.currentTimeMillis();

    public NetworkSender() {
      initClient();
    }

    private void initClient() {
      try {
        transport = new TFramedTransport(new TSocket(host, port, socketTimeout));
        transport.open();
        client = new Scribe.Client(new TBinaryProtocol(transport, false, false));

        LOGGER.info("open transport and initial Scribe.Client completed");
      } catch (Exception e) {
        LOGGER.error(
            "open transport or initial Scribe.Client fail: {}", ExceptionUtils.getStackTrace(e));
        closeSocket();
      }
    }

    @Override
    public void run() {
      // 当标志位为true且队列为空时退出循环
      while (!senderClosed.get() || recordQueue.size() > 0) {
        try {
          LogEntry entry = recordQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
          if (entry != null) {
            entries.add(entry);
          }

          if (entries.size() >= batchSize
              || (System.currentTimeMillis() - lastTime) >= sendInterval) {
            client.Log(entries);

            entries.clear();
            lastTime = System.currentTimeMillis();
          }
        } catch (InterruptedException e) {
          LOGGER.warn("{}", ExceptionUtils.getStackTrace(e));
        } catch (Exception e) {
          LOGGER.warn("send entry error: {}", ExceptionUtils.getStackTrace(e));

          try {
            LOGGER.info(
                "socket maybe timeout, close socket and reconnect, transport.isOpen:{}",
                transport.isOpen());

            closeSocket();
            Thread.sleep(workerSleep);
            initClient();
          } catch (InterruptedException e1) {
            LOGGER.warn("{}", ExceptionUtils.getStackTrace(e1));
          } catch (Exception e1) {
            LOGGER.error("{}", ExceptionUtils.getStackTrace(e1));
          }
        }
      }

      // 发送最后一批数据
      try {
        client.Log(entries);
        LOGGER.debug("last send lines: {}", entries.size());
      } catch (Exception e) {
        LOGGER.warn("send entry error: {}", ExceptionUtils.getStackTrace(e));
      }

      closeSocket();
    }

    private void closeSocket() {
      if (transport != null) {
        transport.close();
      }
      LOGGER.info("transport closed");
    }
  }
}

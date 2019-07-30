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

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.weibo.dip.databus.sink.ScribeSinkConfContants.*;
import static com.weibo.dip.databus.source.FileSourceConfConstants.THREAD_POOL_AWAIT_TIMEOUT;

/** Created by jianhong1 on 2019-07-10. */
public class ScribeSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScribeSink.class);
  private final AtomicBoolean senderClosed = new AtomicBoolean(false);
  private ExecutorService sender;
  private LinkedBlockingQueue<LogEntry> recordQueue = new LinkedBlockingQueue<>();
  private String host;
  private int port;
  private int batchSize;
  private int sendInterval;
  private int capacity;
  private int threadNumber;
  private int workerSleep;
  private int checkInterval;
  private ConcurrentHashMap<Integer, TTransport> sockets = new ConcurrentHashMap<>();
  private ScheduledExecutorService socketManager;

  @Override
  public void process(Message message) throws Exception {
    if (StringUtils.isEmpty(message.getData())) {
      return;
    }
    LogEntry entry = new LogEntry(message.getTopic(), message.getData());

    if (recordQueue.size() > capacity) {
      Thread.sleep(1000);
    } else {
      recordQueue.put(entry);
    }
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

    checkInterval = conf.getInteger(CHECK_INTERVAL, DEFAULT_CHECK_INTERVAL);
    LOGGER.info("Property: {}={}", CHECK_INTERVAL, checkInterval);

    metric.gauge(MetricRegistry.name(name, "recordQueue", "size"), () -> recordQueue.size());
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    socketManager =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("socketManager-pool-%d").build());
    socketManager.scheduleWithFixedDelay(new SocketManagerRunnable(), 0, checkInterval, TimeUnit.SECONDS);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      LOGGER.warn("thread is sleeping, but interrupted: {}", ExceptionUtils.getStackTrace(e));
    }

    sender =
        Executors.newFixedThreadPool(
            threadNumber, new ThreadFactoryBuilder().setNameFormat("sender-pool-%d").build());
    for (int index = 0; index < threadNumber; index++) {
      sender.execute(new NetworkSender(index));
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    socketManager.shutdown();
    try {
      while (!socketManager.awaitTermination(THREAD_POOL_AWAIT_TIMEOUT, TimeUnit.SECONDS)) {
        LOGGER.info("{} socketManager await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} socketManager await termination, but interrupted", name);
    }
    LOGGER.info("{} socketManager stopped", name);

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

    LOGGER.info("{} stopped", name);
  }

  public class SocketManagerRunnable implements Runnable {

    @Override
    public void run() {
      LOGGER.info("socket number: {}", sockets.size());

      // 从map中删除已经关闭的socket
      for (Map.Entry<Integer, TTransport> item : sockets.entrySet()) {
        TTransport transport = item.getValue();
        if (!transport.isOpen()) {
          transport.close();
          sockets.remove(item.getKey());
          LOGGER.warn("socket-{} is closed", item.getKey());
        }
      }

      // 补全map的socket连接
      for (int index = 0; index < threadNumber; index++) {
        if (sockets.containsKey(index)) {
          continue;
        }

        try {
          TTransport transport = new TFramedTransport(new TSocket(new Socket(host, port)));
          sockets.put(index, transport);

          LOGGER.info("initial socket-{} completed", index);
        } catch (Exception e) {
          LOGGER.error("initial socket-{} fail: {}", index, ExceptionUtils.getStackTrace(e));
        }
      }
    }
  }

  public class NetworkSender implements Runnable {
    int index;
    TTransport transport;
    List<LogEntry> entries = new ArrayList<>();
    long lastTime = System.currentTimeMillis();

    public NetworkSender(int index) {
      this.index = index;
    }

    @Override
    public void run() {
      transport = sockets.get(index);
      Scribe.Client client = new Scribe.Client(new TBinaryProtocol(transport));
      LOGGER.info("initial Scribe.Client: {}-{}-{}", client, transport, index);

      while (!senderClosed.get()) {
        try {
          LOGGER.info("{}", recordQueue.size());
          LogEntry entry = recordQueue.poll(2000, TimeUnit.MILLISECONDS);
          LOGGER.info("{}", entry);

          if (entry != null) {
            entries.add(entry);
          }

          if (entries.size() >= batchSize
              || ((System.currentTimeMillis() - lastTime) >= sendInterval && entries.size() > 0)) {

            LOGGER.info("send entries: {}", entries.size());
            client.Log(entries);

            entries.clear();
            lastTime = System.currentTimeMillis();
          }
          LOGGER.info("{}", entries.size());

        } catch (Exception e) {
          LOGGER.warn("socket may be close: {}", ExceptionUtils.getStackTrace(e));
          try {
            Thread.sleep(workerSleep);
          } catch (InterruptedException e1) {
            LOGGER.warn("{} sender await termination, but interrupted", name);
          }
          transport = sockets.get(index);
          client = new Scribe.Client(new TBinaryProtocol(transport));
          LOGGER.info(
              "reconnect socket and initial Scribe.Client: {}-{}-{}", client, transport, index);
        }
      }

      if (transport != null) {
        transport.close();
        LOGGER.info("{} closed", transport);
      }
    }
  }
}

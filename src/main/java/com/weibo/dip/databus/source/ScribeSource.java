package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.source.scribe.LogEntry;
import org.apache.flume.source.scribe.ResultCode;
import org.apache.flume.source.scribe.Scribe;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianhong1 on 2018/5/28.
 */
public class ScribeSource extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScribeSource.class);
  private static final String SCRIBE_SERVER_PORT = "scribe.server.port";
  private static final String SCRIBE_SERVER_THREADS = "scribe.server.threads";
  private static final int MAX_READ_BUFFER_BYTES = 134217728;

  private String port;
  private String threads;

  private Thread thread;
  private THsHaServer server;

  @Override
  public void setConf(Configuration conf) throws Exception {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    port = conf.get(SCRIBE_SERVER_PORT);
    Preconditions.checkState(StringUtils.isNumeric(port),
        name + " " + SCRIBE_SERVER_PORT + " must be numeric");
    LOGGER.info("Property: {}={}", SCRIBE_SERVER_PORT, port);

    threads = conf.get(SCRIBE_SERVER_THREADS);
    Preconditions.checkState(StringUtils.isNumeric(threads),
        name + " " + SCRIBE_SERVER_THREADS + " must be numeric");
    LOGGER.info("Property: {}={}", SCRIBE_SERVER_THREADS, threads);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    thread = new Thread(new Startup());
    thread.start();

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      LOGGER.warn("thread is sleeping, but interrupted \n{}", ExceptionUtils.getFullStackTrace(e));
    }

    if (!server.isServing()) {
      throw new IllegalStateException("Failed initialization of ScribeSource");
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    if (server != null) {
      server.stop();
    }

    try {
      thread.join();
    } catch (InterruptedException e) {
      LOGGER.warn("{} {} waiting for stop, but interrupted", name, thread.getName());
    }

    LOGGER.info("{} stopped", name);
  }

  private class Startup extends Thread {

    @Override
    public void run() {
      try {
        Scribe.Processor processor = new Scribe.Processor(new Receiver());
        TNonblockingServerTransport transport =
            new TNonblockingServerSocket(Integer.parseInt(port));
        THsHaServer.Args args = new THsHaServer.Args(transport);

        args.minWorkerThreads(Integer.parseInt(threads));
        args.processor(processor);
        args.transportFactory(new TFramedTransport.Factory(MAX_READ_BUFFER_BYTES));
        args.protocolFactory(new TBinaryProtocol.Factory(false, false));

        server = new THsHaServer(args);

        LOGGER.info("Starting Scribe Server on port {}", port);

        //执行serve方法，线程会阻塞
        server.serve();

        LOGGER.info("Scribe Server stopped on port {}", port);
      } catch (TTransportException e) {
        LOGGER.warn("Scribe failed {}", ExceptionUtils.getFullStackTrace(e));
      }
    }
  }

  private class Receiver implements Scribe.Iface {

    @Override
    public ResultCode Log(List<LogEntry> messages) {
      if (messages != null) {
        try {
          for (LogEntry entry : messages) {
            String category = entry.getCategory();
            String message = entry.getMessage();

            deliver(new Message(category, message));
          }
          return ResultCode.OK;
        } catch (Exception e) {
          LOGGER.warn("Scribe source handling failure: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }
      return ResultCode.TRY_LATER;
    }
  }
}
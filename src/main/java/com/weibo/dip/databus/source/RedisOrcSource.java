package com.weibo.dip.databus.source;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.redis.RedisClient;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Source;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianhong1 on 2018/5/28.
 */
public class RedisOrcSource extends Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisOrcSource.class);

  private static final String REDIS_HOST = "source.redis.host";
  private static final String REDIS_PORT = "source.redis.port";
  private static final String THREADS_NUMBER = "source.threads.number";
  private static final String LIST_NAME = "source.redis.list.name";
  private static final String CATEGORY = "Category";
  private static final long THREAD_SLEEP_TIME_MILLISECOND = 60000;
  private static final long AWAIT_TERMINATION_TIME = 30;
  private boolean flag = true;

  private String redisHost;
  private String redisPort;
  private String threadsNumber;
  private String listName;

  private ExecutorService executorService;
  private RedisClient redisClient;

  @Override
  public void setConf(Configuration conf) {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    redisHost = conf.get(REDIS_HOST);
    Preconditions.checkState(StringUtils.isNotEmpty(redisHost),
        name + " " + REDIS_HOST + " must be specified");
    LOGGER.info("Property: {}={}", REDIS_HOST, redisHost);

    redisPort = conf.get(REDIS_PORT);
    Preconditions.checkState(StringUtils.isNumeric(redisPort),
        name + " " + REDIS_PORT + " must be numeric");
    LOGGER.info("Property: {}={}", REDIS_PORT, redisPort);

    threadsNumber = conf.get(THREADS_NUMBER);
    Preconditions.checkState(StringUtils.isNumeric(threadsNumber),
        name + " " + THREADS_NUMBER + " must be numeric");
    LOGGER.info("Property: {}={}", THREADS_NUMBER, threadsNumber);

    listName = conf.get(LIST_NAME);
    Preconditions.checkState(StringUtils.isNotEmpty(listName),
        name + " " + LIST_NAME + " must be specified");
    LOGGER.info("Property: {}={}", LIST_NAME, listName);
  }

  @Override
  public void start() {
    LOGGER.info("{} starting...", name);

    redisClient = new RedisClient(redisHost, Integer.parseInt(redisPort));
    LOGGER.info("connect redis success, {}:{}", redisHost, redisPort);

    int numThreads = Integer.parseInt(threadsNumber);
    executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.execute(new Streamer(redisClient));
    }

    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopping...", name);

    flag = false;

    executorService.shutdown();
    try {
      while (!executorService.awaitTermination(AWAIT_TERMINATION_TIME, TimeUnit.SECONDS)) {
        LOGGER.info("{} executors await termination", name);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} executors await termination, but interrupt", name);
    }

    if (redisClient != null) {
      redisClient.close();
      LOGGER.info("jedisPool closed ");
    }

    LOGGER.info("{} stopped", name);
  }

  public class Streamer implements Runnable {
    private RedisClient redisClient;

    private Streamer(RedisClient redisClient) {
      this.redisClient = redisClient;
    }

    @Override
    public void run() {
      String threadName = Thread.currentThread().getName();
      LOGGER.info(name + " " + threadName + " started");

      while (flag) {

        String jsonData = redisClient.lpop(listName);
        if (jsonData != null) {
          Map<String, Object> map = GsonUtil.fromJson(jsonData, GsonUtil.GsonType.OBJECT_MAP_TYPE);
          String category = map.get(CATEGORY).toString();
          deliver(new Message(category, jsonData));
        } else {
          try {
            LOGGER.info("redis list:{} is empty, {} sleeping", listName, threadName);
            Thread.sleep(THREAD_SLEEP_TIME_MILLISECOND);
          } catch (InterruptedException e) {
            LOGGER.warn("thread:{} is sleeping, but interrupted", threadName);
          }
        }
      }

      LOGGER.info(name + " " + threadName + " stopped");
    }
  }
}

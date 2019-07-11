package com.weibo.dip;

import com.weibo.dip.databus.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianhong1 on 2019-07-05.
 */
public class TestScheduleExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestScheduleExecutor.class);

  public static void main(String[] args) {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleWithFixedDelay(new MyRunnable(), 1, 2, TimeUnit.SECONDS);
  }

   static class MyRunnable implements Runnable{
    int n = 0;

    @Override
    public void run() {
      if(n == 2){
        n++;
        throw new RuntimeException("woca");
      }
      LOGGER.info("{}", n++);
    }
  }
}

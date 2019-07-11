package com.weibo.dip;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class TestThreadPool {
  public static void main(String[] args) {
    int threadNumber = 3;
    ExecutorService fileReader =
        Executors.newFixedThreadPool(
            threadNumber, new ThreadFactoryBuilder().setNameFormat("fileReader-pool-%d").build());
    for (int i = 0; i < 5; i++) {
      fileReader.execute(new FileReaderRunnable());
    }
  }

  static class FileReaderRunnable implements Runnable{

    @Override
    public void run() {
      while (true){
        try{
        Thread.sleep(1000);
        }catch (Exception e){

        }
        System.out.println(Thread.currentThread().getName());
      }
    }
  }
}

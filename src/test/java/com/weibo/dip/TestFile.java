package com.weibo.dip;

import com.weibo.dip.databus.source.FileStatus;

import java.io.File;
import java.io.IOException;

/**
 * Created by jianhong1 on 2019-07-08.
 */
public class TestFile {
  public static void main(String[] args) {
    File positionFile = new File(String.format("%s/%s.position2", "/var/log/falcon", "test"));
    try {
      positionFile.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    File targetDirectory = new File("/var/log/falcon");
    for (File item : targetDirectory.listFiles()){
      if(item.isFile()){
        if(item.getName().matches("^.*\\.log$")){
          System.out.println(item.getName());
          System.out.println(item);
        }
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("adf").append("\n");
//    sb.deleteCharAt(sb.length() -1);
    System.out.println(sb.toString());
    System.out.println("end");
  }
}

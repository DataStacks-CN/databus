package com.weibo.dip;

import com.weibo.dip.databus.source.FileStatus;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jianhong1 on 2019-07-10.
 */
public class TestRegex {
  private static final String FILE_STATUS_PATTERN = "(\\S+) (\\S+) (\\S+)";
  public static void main(String[] args) {

    Pattern p = Pattern.compile(FILE_STATUS_PATTERN);
    Matcher m = p.matcher("/var/log/falcon/cdn-monitor.log 6785 true");

    if (m.find()) {
      FileStatus fileStatus = new FileStatus();
      fileStatus.setPath(m.group(1));
      fileStatus.setOffset(Long.valueOf(m.group(2)));
      fileStatus.setCompleted(Boolean.getBoolean(m.group(3)));

      System.out.println(m.group(3));
      System.out.println(Boolean.parseBoolean("true"));
      System.out.println(Boolean.getBoolean(m.group(3)));
      System.out.println(fileStatus);
    }
  }
}

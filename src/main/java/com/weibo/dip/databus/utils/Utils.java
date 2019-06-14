package com.weibo.dip.databus.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Created by zhiqiang on 2018/12/11. */
public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static String getIp() {
    String ip = "";
    try {
      ip = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOGGER.warn("get ip address error :{}", e);
    }
    return ip;
  }
}

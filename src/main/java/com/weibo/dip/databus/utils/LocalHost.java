package com.weibo.dip.databus.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by jianhong1 on 2020-05-19.
 */
public class LocalHost {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalHost.class);

  public static String getHostname(){
    String hostname = null;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOGGER.warn("get hostname fail: {}", ExceptionUtils.getStackTrace(e));
    }
    return hostname;
  }

  public static String getIp(){
    String ip = null;
    try {
      ip = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOGGER.warn("get ip fail: {}", ExceptionUtils.getStackTrace(e));
    }
    return ip;
  }
}

package com.weibo.dip.databus.core;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.WatchAlert;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by jianhong1 on 2018/12/3. */
public class Alarm {
  private static final Logger LOGGER = LoggerFactory.getLogger(Alarm.class);

  private static final String INTERVAL_MS = "alarm.interval.ms";
  private static final String SERVICE = "alarm.service";
  private static final String SUB_SERVICE = "alarm.sub.service";
  private static final String GROUPS = "alarm.groups";
  private static final String ALARM_ENABLE = "alarm.enable";

  private static long errorTotal = 0;
  private static long startTime = 0;
  private static String intervalMs;
  private static String service;
  private static String subService;
  private static String[] groups;
  private static String alarmEnable;
  private static String hostname;

  static {
    try {
      Configuration alarmConf =
          new Configuration(
              Alarm.class.getClassLoader().getResourceAsStream(Constants.ALARM_PROPERTIES));

      intervalMs = alarmConf.get(INTERVAL_MS);
      Preconditions.checkState(StringUtils.isNumeric(intervalMs), INTERVAL_MS + " must be numeric");
      LOGGER.info("Property: {}={}", INTERVAL_MS, intervalMs);

      service = alarmConf.get(SERVICE);
      Preconditions.checkState(StringUtils.isNotEmpty(service), SERVICE + " must be specified");
      LOGGER.info("Property: {}={}", SERVICE, service);

      subService = alarmConf.get(SUB_SERVICE);
      Preconditions.checkState(
          StringUtils.isNotEmpty(subService), SUB_SERVICE + " must be specified");
      LOGGER.info("Property: {}={}", SUB_SERVICE, subService);

      Preconditions.checkState(
          StringUtils.isNotEmpty(alarmConf.get(GROUPS)), GROUPS + " must be specified");
      groups = alarmConf.get(GROUPS).split(Constants.COMMA);
      LOGGER.info("Property: {}={}", GROUPS, alarmConf.get(GROUPS));

      alarmEnable = alarmConf.get(ALARM_ENABLE);
      Preconditions.checkState(
          StringUtils.isNotEmpty(alarmEnable), ALARM_ENABLE + " must be specified");
      LOGGER.info("Property: {}={}", ALARM_ENABLE, alarmEnable);

      hostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      alarmEnable = "false";
      LOGGER.error("load alarm conf failed: \n{}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /** 发送报警. */
  public static synchronized void sendAlarm() {
    // 统计丢失消息数
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    errorTotal++;

    // 每隔 intervalMs 发送一次报警
    long endTime = System.currentTimeMillis();
    if (endTime - startTime >= Long.parseLong(intervalMs)) {
      try {
        String content =
            String.format("%s deliver message failed number: %s", hostname, errorTotal);
        WatchAlert.sendAlarmToGroups(service, subService, content, content, groups);
        LOGGER.error(content);
        errorTotal = 0;
        startTime = 0;
      } catch (Exception e) {
        LOGGER.error("send alert failed, {}", ExceptionUtils.getFullStackTrace(e));
      }
    }
  }

  /**
   * 封装 WatchAlert sendAlarmToGroups()方法.
   */
  public static void sendAlarm(String subject, String content) {
    try {
      WatchAlert.sendAlarmToGroups(
          service, subService, hostname + " " + subject, hostname + " " + content, groups);
    } catch (Exception e) {
      LOGGER.error("send alert failed, {}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 判断是否发送报警.
   *
   * @return true or false
   */
  public static boolean alarmEnable() {
    return alarmEnable.equals("true") || alarmEnable.equals("True");
  }
}

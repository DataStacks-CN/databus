package com.weibo.dip.databus.core;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.weibo.dip.databus.persist.ElasticSearchReporter;
import com.weibo.dip.databus.utils.Utils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Created by yurun on 17/8/31. */
public class Metric implements Configurable, Lifecycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

  private static final String METRIC_PERSIST_INTERVAL = "metric.persist.interval";
  private static final String METRIC_PERSIST_CLASS = "metric.persist.class";
  private static final String ES_INDEX = "metric.es.index";
  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  private static final Metric METRIC = new Metric();
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<String, AtomicLong> metrics = new HashMap<>();
  private Saver saver;
  private long persistInterval;
  private Persist persister;
  private ElasticSearchReporter reporter;

  /**
   * ----------------------------------------
   * ES_METRICS 往ES中写入的所有Metric.
   * ----------------------------------------
   * List : List<com.weibo.dip.data.platform.commons.metric.Metric>
   */
  private static final List<List<com.weibo.dip.data.platform.commons.metric.Metric<?>>> ES_METRICS =
      new ArrayList<>();

  /**
   * ----------------------------------------
   * ES_METRIC_MAPPING metric索引与往ES中写入的Metric对应关系
   * ----------------------------------------
   * key : metric index name
   * value : com.weibo.dip.data.platform.commons.metric.Metric
   */
  private static final ConcurrentHashMap<
          String, com.weibo.dip.data.platform.commons.metric.Metric<?>>
      ES_METRIC_MAPPING = new ConcurrentHashMap<>();

  private Metric() {}

  public static Metric getInstance() {
    return METRIC;
  }

  @Override
  public void setConf(Configuration conf) throws Exception {
    String persistIntervalStr = conf.get(METRIC_PERSIST_INTERVAL);
    LOGGER.info("metric persist interval: {}", persistIntervalStr);
    Preconditions.checkState(
        StringUtils.isNotEmpty(persistIntervalStr), METRIC_PERSIST_INTERVAL + " must be specified");

    persistInterval = Long.valueOf(conf.get(METRIC_PERSIST_INTERVAL));
    Preconditions.checkState(
        persistInterval > 0, METRIC_PERSIST_INTERVAL + " must be greater than zero");

    String persistName = conf.get(METRIC_PERSIST_CLASS);
    LOGGER.info("metric persist class: {}", persistName);
    Preconditions.checkState(
        StringUtils.isNotEmpty(persistName), METRIC_PERSIST_CLASS + " must be specified");

    persister = (Persist) Class.forName(persistName).newInstance();
    persister.setConf(conf);

    saver = new Saver();

    String esIndex = conf.get(ES_INDEX);
    LOGGER.info("{} : {}", ES_INDEX, esIndex);

    Preconditions.checkState(StringUtils.isNotEmpty(esIndex), ES_INDEX + " must be specified");

    reporter =
        ElasticSearchReporter.forRegistry(METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .setEsIndex(esIndex)
            .setEsMetricMapping(ES_METRIC_MAPPING)
            .setEsMetrics(ES_METRICS)
            .build();
  }

  @Override
  public void start() {
    LOGGER.info("metric starting...");

    LOGGER.info("metric.persister starting...");
    persister.start();
    LOGGER.info("metric.persister started");

    LOGGER.info("metric saver starting...");
    saver.start();
    LOGGER.info("metric saver started");

    LOGGER.info("metric reporter starting...");
    reporter.start(1, TimeUnit.MINUTES);
    LOGGER.info("metric reporter started");

    LOGGER.info("metric started");
  }

  @Override
  public void stop() {

    LOGGER.info("metric stopping...");

    LOGGER.info("metric.saver stopping");
    saver.interrupt();
    try {
      saver.join();
    } catch (InterruptedException e) {
      LOGGER.warn("metric saver await for termination, but interrupted");
    }
    LOGGER.info("metric.saver stopped");

    LOGGER.info("metric.persister stopping...");
    persister.stop();
    LOGGER.info("metric.perssiter stopped");

    LOGGER.info("metric reporter stopping...");
    reporter.close();
    LOGGER.info("metric reporter stopped");

    LOGGER.info("metric stopped");
  }

  public void increment(String name, String topic, long delta) {
    String counterName = name + Constants.COLON + topic;

    lock.readLock().lock();

    if (!metrics.containsKey(counterName)) {
      lock.readLock().unlock();

      lock.writeLock().lock();

      try {
        if (!metrics.containsKey(counterName)) {
          metrics.put(counterName, new AtomicLong(0L));
        }

        lock.readLock().lock();
      } finally {
        lock.writeLock().unlock();
      }
    }

    try {
      metrics.get(counterName).addAndGet(delta);
    } finally {
      lock.readLock().unlock();
    }
  }

  public interface Persist extends Configurable, Lifecycle {

    void persist(List<Counter> counters);
  }

  public static class Counter {

    private String name;
    private String topic;
    private long delta;

    public Counter() {}

    private Counter(String name, String topic, long delta) {
      this.name = name;
      this.topic = topic;
      this.delta = delta;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public long getDelta() {
      return delta;
    }

    public void setDelta(long delta) {
      this.delta = delta;
    }

    @Override
    public String toString() {
      return "Counter{"
          + "name='"
          + name
          + '\''
          + ", topic='"
          + topic
          + '\''
          + ", delta="
          + delta
          + '}';
    }
  }

  private class Saver extends Thread {

    private void persist(Map<String, AtomicLong> metrics) {
      if (MapUtils.isEmpty(metrics)) {
        return;
      }

      lock.writeLock().lock();

      List<Counter> counters = new ArrayList<>();

      for (Map.Entry<String, AtomicLong> entry : metrics.entrySet()) {
        String[] keys = entry.getKey().split(Constants.COLON);
        long counter = entry.getValue().get();

        String name = keys[0];
        String topic = keys[1];

        counters.add(new Counter(name, topic, counter));
      }

      metrics.clear();

      lock.writeLock().unlock();

      try {
        persister.persist(counters);
      } catch (Exception e) {
        LOGGER.error("metric persist error: " + ExceptionUtils.getFullStackTrace(e));
      }
    }

    @Override
    public void run() {
      while (!isInterrupted()) {
        try {
          Thread.sleep(persistInterval);
        } catch (InterruptedException e) {
          LOGGER.info("metric saver persist sleep, but interrupted");
          break;
        }

        persist(metrics);
      }

      persist(metrics);
    }
  }

  private com.codahale.metrics.Counter counter(String name) {
    return METRIC_REGISTRY.counter(name);
  }

  private Meter meter(String name) {
    return METRIC_REGISTRY.meter(name);
  }

  /**
   * 通过指标名称获取对应metric，然后进行对应统计.
   * SuccessCount : sink/source 成功读取/写入条数
   * ByteRate : sink/source 读取/写入字节速率
   * RecordsRate : sink/source 读取/写入记录条数速率
   */
  public void collectSuccessCountAndRate(String name, Message message, String type) {

    String common = name + Constants.HYPHEN + message.getTopic() + Constants.HYPHEN + type;
    String successCountMetricName = common + "SuccessCount";
    com.codahale.metrics.Counter counter =
        (com.codahale.metrics.Counter) getMetricByName(successCountMetricName);
    counter.inc();

    String byteRateMetricName = common + "ByteRate";
    Meter byteRateMeter = (Meter) getMetricByName(byteRateMetricName);
    byteRateMeter.mark(message.getData().length());

    String recordsRateMetricName = common + "RecordsRate";
    Meter countRateMeter = (Meter) getMetricByName(recordsRateMetricName);
    countRateMeter.mark();
  }

  /**
   * 通过指标名称获取对应metric，然后进行对应统计.
   * FailCount : sink 写入失败记录条数
   */
  void collectSinkFailCount(String name, Message message) {
    String failCountMetricName =
        name + Constants.HYPHEN + message.getTopic() + Constants.HYPHEN + "SinkFailCount";
    com.codahale.metrics.Counter counter =
        (com.codahale.metrics.Counter) getMetricByName(failCountMetricName);
    counter.inc();
  }

  /** source-sink metric 配置入口. */
  public void config(Object object, Configuration conf) {

    if ((object instanceof Source)) {
      loadSourceMetricConf((Source) object, conf);
    } else if (object instanceof Sink) {
      loadSinkMetricConf((Sink) object, conf);
    }
  }

  /**
   * 加载 source 采集指标.
   * SourceSuccessCount : 读取成功记录数
   * SourceByteRate : 读取字节速度
   * SourceRecordsRate :读取记录条数速度
   */
  private void loadSourceMetricConf(Source source, Configuration conf) {

    final Logger logger = LoggerFactory.getLogger(source.getClass());
    List<com.weibo.dip.data.platform.commons.metric.Metric<?>> metricListPrefix = new ArrayList<>();
    metricListPrefix.add(
        com.weibo.dip.data.platform.commons.metric.Metric.newEntity("ip", Utils.getIp()));
    metricListPrefix.add(
        com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
            "pipeline", conf.get(Constants.PIPELINE_NAME)));
    metricListPrefix.add(
        com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
            "source", source.getClass().getSimpleName()));

    String[] topicAndThreads = conf.get("topic.and.threads").split(Constants.COMMA);
    String sourceClassName = source.getClass().getSimpleName();
    Arrays.stream(topicAndThreads)
        .forEach(
            item -> {
              List<com.weibo.dip.data.platform.commons.metric.Metric<?>> metricList =
                  new ArrayList();
              String topic = item.split(Constants.COLON)[0];
              String metricNamePrefix =
                  conf.get(Constants.PIPELINE_NAME)
                      + Constants.HYPHEN
                      + sourceClassName
                      + Constants.HYPHEN
                      + topic
                      + Constants.HYPHEN;

              com.weibo.dip.data.platform.commons.metric.Metric<String> topicMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity("topic", topic);
              metricList.add(topicMetric);

              String sourceSuccessCountMetricName = metricNamePrefix + "SourceSuccessCount";
              counter(sourceSuccessCountMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Long> sourceSuccessCountMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
                      "SourceSuccessCount", 0);
              metricList.add(sourceSuccessCountMetric);
              ES_METRIC_MAPPING.putIfAbsent(sourceSuccessCountMetricName, sourceSuccessCountMetric);

              String sourceByteRateMetricName = metricNamePrefix + "SourceByteRate";
              meter(sourceByteRateMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Float> sourceByteRateMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity("SourceByteRate", 0f);
              metricList.add(sourceByteRateMetric);
              ES_METRIC_MAPPING.putIfAbsent(sourceByteRateMetricName, sourceByteRateMetric);

              String sourceRecordsRateMetricName = metricNamePrefix + "SourceRecordsRate";
              meter(sourceRecordsRateMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Float> sourceRecordsRateMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
                      "SourceRecordsRate", 0f);
              metricList.add(sourceRecordsRateMetric);
              ES_METRIC_MAPPING.putIfAbsent(sourceRecordsRateMetricName, sourceRecordsRateMetric);

              List<com.weibo.dip.data.platform.commons.metric.Metric<?>> allMetricList =
                  new ArrayList();
              allMetricList.addAll(metricListPrefix);
              allMetricList.addAll(metricList);
              ES_METRICS.add(allMetricList);

              logger.info("{} Metric Name : {}", sourceClassName, sourceSuccessCountMetricName);
              logger.info("{} Metric Name : {}", sourceClassName, sourceByteRateMetricName);
              logger.info("{} Metric Name : {}", sourceClassName, sourceRecordsRateMetricName);
            });
  }

  /**
   * 加载 sink 采集指标.
   * SinkSuccessCount : 写入成功记录数
   * SinkByteRate : 写入字节速度
   * SinkRecordsRate : 写入记录条数速度
   * SinkFailCount : 写入失败记录数
   */
  private void loadSinkMetricConf(Sink sink, Configuration conf) {
    final Logger logger = LoggerFactory.getLogger(sink.getClass());
    String tm = conf.get("topic.mappings");
    String[] mappings = tm == null || tm.trim().length() == 0 ? null : tm.split(Constants.COMMA);
    Map<String, String> maps = new HashMap<>(16);
    if (mappings != null) {
      Arrays.stream(mappings)
          .forEach(
              item -> {
                String[] s = item.split(Constants.COLON);
                maps.putIfAbsent(s[0], s[1]);
              });
    }
    List<com.weibo.dip.data.platform.commons.metric.Metric<?>> metricListPrefix = new ArrayList<>();
    metricListPrefix.add(
        com.weibo.dip.data.platform.commons.metric.Metric.newEntity("ip", Utils.getIp()));
    metricListPrefix.add(
        com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
            "pipeline", conf.get(Constants.PIPELINE_NAME)));
    metricListPrefix.add(
        com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
            "sink", sink.getClass().getSimpleName()));
    String[] topicAndThreads = conf.get("topic.and.threads").split(Constants.COMMA);
    String sinkClassName = sink.getClass().getSimpleName();
    Arrays.stream(topicAndThreads)
        .forEach(
            item -> {
              List<com.weibo.dip.data.platform.commons.metric.Metric<?>> metricList =
                  new ArrayList();
              String topic = item.split(Constants.COLON)[0];
              String sinkTopic = maps.get(topic) == null ? topic : maps.get(topic);
              String metricNamePrefix =
                  conf.get(Constants.PIPELINE_NAME)
                      + Constants.HYPHEN
                      + sinkClassName
                      + Constants.HYPHEN
                      + sinkTopic
                      + Constants.HYPHEN;

              com.weibo.dip.data.platform.commons.metric.Metric<String> topicMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity("topic", sinkTopic);
              metricList.add(topicMetric);

              String sinkSuccessCountMetricName = metricNamePrefix + "SinkSuccessCount";
              counter(sinkSuccessCountMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Long> sinkSuccessCountMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity(
                      "SinkSuccessCount", 0);
              metricList.add(sinkSuccessCountMetric);
              ES_METRIC_MAPPING.putIfAbsent(sinkSuccessCountMetricName, sinkSuccessCountMetric);

              String sinkByteRateMetricName = metricNamePrefix + "SinkByteRate";
              meter(sinkByteRateMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Float> sinkByteRateMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity("SinkByteRate", 0f);
              metricList.add(sinkByteRateMetric);
              ES_METRIC_MAPPING.putIfAbsent(sinkByteRateMetricName, sinkByteRateMetric);

              String sinkRecordsRateMetricName = metricNamePrefix + "SinkRecordsRate";
              meter(sinkRecordsRateMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Float> sinkRecordsRateMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity("SinkRecordsRate", 0f);
              metricList.add(sinkRecordsRateMetric);
              ES_METRIC_MAPPING.putIfAbsent(sinkRecordsRateMetricName, sinkRecordsRateMetric);

              String sinkFailCountMetricName = metricNamePrefix + "SinkFailCount";
              counter(sinkFailCountMetricName);
              com.weibo.dip.data.platform.commons.metric.Metric<Long> sinkFailCountMetric =
                  com.weibo.dip.data.platform.commons.metric.Metric.newEntity("SinkFailCount", 0);
              metricList.add(sinkFailCountMetric);
              ES_METRIC_MAPPING.putIfAbsent(sinkFailCountMetricName, sinkFailCountMetric);

              List<com.weibo.dip.data.platform.commons.metric.Metric<?>> allMetricList =
                  new ArrayList<>();
              allMetricList.addAll(metricListPrefix);
              allMetricList.addAll(metricList);
              ES_METRICS.add(allMetricList);

              logger.info("{} Metric Name : {}", sinkClassName, sinkSuccessCountMetricName);
              logger.info("{} Metric Name : {}", sinkClassName, sinkByteRateMetricName);
              logger.info("{} Metric Name : {}", sinkClassName, sinkRecordsRateMetricName);
              logger.info("{} Metric Name : {}", sinkClassName, sinkFailCountMetricName);
            });
  }

  private com.codahale.metrics.Metric getMetricByName(String metricName) {
    return METRIC_REGISTRY.getMetrics().get(metricName);
  }
}

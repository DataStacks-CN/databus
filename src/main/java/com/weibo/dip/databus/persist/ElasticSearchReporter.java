package com.weibo.dip.databus.persist;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.weibo.dip.data.platform.commons.metric.Metric;
import com.weibo.dip.data.platform.commons.metric.MetricStore;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.List;
import java.util.Locale;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Created by zhiqiang on 18/12/1. */
public class ElasticSearchReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchReporter.class);

  public static ElasticSearchReporter.Builder forRegistry(MetricRegistry registry) {
    return new ElasticSearchReporter.Builder(registry);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private Locale locale;
    private Clock clock;
    private TimeZone timeZone;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String esIndex;
    private List<List<com.weibo.dip.data.platform.commons.metric.Metric<?>>> esMetrics;
    private ConcurrentHashMap<String, com.weibo.dip.data.platform.commons.metric.Metric<?>>
        esMetricMapping;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.locale = Locale.getDefault();
      this.clock = Clock.defaultClock();
      this.timeZone = TimeZone.getDefault();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    public ElasticSearchReporter.Builder formattedFor(Locale locale) {
      this.locale = locale;
      return this;
    }

    public ElasticSearchReporter.Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public ElasticSearchReporter.Builder formattedFor(TimeZone timeZone) {
      this.timeZone = timeZone;
      return this;
    }

    public ElasticSearchReporter.Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public ElasticSearchReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public ElasticSearchReporter.Builder setEsIndex(String esIndex) {
      this.esIndex = esIndex;
      return this;
    }

    public ElasticSearchReporter.Builder setEsMetricMapping(
        ConcurrentHashMap<String, com.weibo.dip.data.platform.commons.metric.Metric<?>>
            esMetricMapping) {
      this.esMetricMapping = esMetricMapping;
      return this;
    }

    public ElasticSearchReporter.Builder setEsMetrics(
        List<List<com.weibo.dip.data.platform.commons.metric.Metric<?>>> esMetrics) {
      this.esMetrics = esMetrics;
      return this;
    }

    public ElasticSearchReporter.Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public ElasticSearchReporter build() {
      return new ElasticSearchReporter(
          registry,
          locale,
          clock,
          timeZone,
          rateUnit,
          durationUnit,
          filter,
          esIndex,
          esMetrics,
          esMetricMapping);
    }
  }

  private final Locale locale;
  private final Clock clock;
  private final DateFormat dateFormat;
  private final String esIndex;
  private final MetricRegistry registry;
  private List<List<com.weibo.dip.data.platform.commons.metric.Metric<?>>> esMetrics;
  private ConcurrentHashMap<String, com.weibo.dip.data.platform.commons.metric.Metric<?>>
      esMetricMapping;

  private ElasticSearchReporter(
      MetricRegistry registry,
      Locale locale,
      Clock clock,
      TimeZone timeZone,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      MetricFilter filter,
      String esIndex,
      List<List<com.weibo.dip.data.platform.commons.metric.Metric<?>>> esMetrics,
      ConcurrentHashMap<String, com.weibo.dip.data.platform.commons.metric.Metric<?>>
          esMetricMapping) {
    super(registry, "es-reporter", filter, rateUnit, durationUnit);
    this.registry = registry;
    this.locale = locale;
    this.clock = clock;
    this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale);
    dateFormat.setTimeZone(timeZone);
    this.esIndex = esIndex;
    this.esMetrics = esMetrics;
    this.esMetricMapping = esMetricMapping;
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    registry
        .getMetrics()
        .forEach(
            (metricName, metric) -> {
              if (metric instanceof Counter) {
                Metric<Long> longMetric = (Metric<Long>) esMetricMapping.get(metricName);
                longMetric.setValue(((Counter) metric).getCount());
              } else if (metric instanceof Meter) {
                Metric<Float> longMetric = (Metric<Float>) esMetricMapping.get(metricName);
                longMetric.setValue(BigDecimal.valueOf(((Meter) metric).getOneMinuteRate()).floatValue());
              }
            });

    esMetrics.forEach(
        entry -> {
          try {
            MetricStore.store(esIndex, timestamp(), entry);
          } catch (Exception e) {
            LOGGER.warn("store metrics to es occur exception :{}", ExceptionUtils.getFullStackTrace(e));
          }
        });
  }

  public long timestamp() {
    return System.currentTimeMillis() / 60000 * 60000;
  }
}

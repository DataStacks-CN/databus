package com.weibo.dip.databus.persist;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Metric;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by yurun on 17/9/1.
 */
public class LoggerPersist implements Metric.Persist {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerPersist.class);

    @Override
    public void setConf(Configuration conf) throws Exception {

    }

    @Override
    public void start() {

    }

    @Override
    public void persist(List<Metric.Counter> counters) {
        if (CollectionUtils.isEmpty(counters)) {
            return;
        }

        for (Metric.Counter counter : counters) {
            LOGGER.info(counter.toString());
        }
    }

    @Override
    public void stop() {

    }

}

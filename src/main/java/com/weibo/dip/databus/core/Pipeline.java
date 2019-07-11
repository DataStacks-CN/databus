package com.weibo.dip.databus.core;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.store.Store;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/8/9.
 */
public class Pipeline implements Configurable, Lifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);

    private long timestamp;

    private Configuration conf;

    private String name;

    private Source source;

    private Converter converter;

    private Store store;

    private Sink sink;

    public Pipeline(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Configuration getConf() {
        return conf;
    }

    public String getName() {
        return name;
    }

    public Source getSource() {
        return source;
    }

    public Sink getSink() {
        return sink;
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        this.conf = conf;

        /*
            pipeline name
         */
        name = conf.get(Constants.PIPELINE_NAME);
        Preconditions.checkState(StringUtils.isNotEmpty(name),
            Constants.PIPELINE_NAME + " can't be empty");

        /*
            source
         */
        String sourceName = conf.get(Constants.PIPELINE_SOURCE);
        Preconditions.checkState(StringUtils.isNotEmpty(sourceName),
            name + " " + Constants.PIPELINE_SOURCE + " can't be empty");

        source = (Source) Class.forName(sourceName).newInstance();
        source.setConf(conf);

        /*
           converter
         */
        String converterName = conf.get(Constants.PIPELINE_CONVERTER);
        Preconditions.checkState(StringUtils.isNotEmpty(converterName),
            name + " " + Constants.PIPELINE_CONVERTER + " can't be empty");

        converter = (Converter) Class.forName(converterName).newInstance();
        converter.setConf(conf);

        /*
            store
         */
        String storeName = conf.get(Constants.PIPELINE_STORE);
        Preconditions.checkState(StringUtils.isNotEmpty(storeName),
            Constants.PIPELINE_STORE + " can't be empty");

        store = (Store) Class.forName(storeName).newInstance();
        store.setConf(conf);

        /*
            sink
         */
        String sinkName = conf.get(Constants.PIPELINE_SINK);
        Preconditions.checkState(StringUtils.isNotEmpty(sinkName),
            Constants.PIPELINE_SINK + " can't be empty");

        sink = (Sink) Class.forName(sinkName).newInstance();
        sink.setConf(conf);

        /*
            link
         */
        source.link(converter);
        source.link(sink);

        sink.link(store);
        store.link(sink);
    }

    @Override
    public void start() {
        LOGGER.info("pipeline {} starting...", name);

        sink.start();
        store.start();
        converter.start();
        source.start();

        LOGGER.info("pipeline {} started", name);
    }

    @Override
    public void stop() {
        LOGGER.info("pipeline {} stoping...", name);

        source.stop();
        converter.stop();
        store.stop();
        sink.stop();

        LOGGER.info("pipeline " + name + " stoped");
    }

}

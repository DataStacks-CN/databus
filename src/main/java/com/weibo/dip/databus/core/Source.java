package com.weibo.dip.databus.core;

import java.util.Objects;

/**
 * Created by yurun on 17/8/9.
 */
public abstract class Source implements Configurable, Lifecycle, Deliverable {

    protected String name;

    protected Converter converter;

    protected Sink sink;

    protected Metric metric = Metric.getInstance();

    public Source() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void link(Converter converter) {
        this.converter = converter;
    }

    public void link(Sink sink) {
        this.sink = sink;
    }

    @Override
    public void deliver(Message message) {
        if (Objects.isNull(message)) {
            return;
        }

        metric.increment(name, message.getTopic(), 1L);

        message = converter.convert(message);
        if (Objects.isNull(message)) {
            return;
        }

        sink.deliver(message);
    }

}

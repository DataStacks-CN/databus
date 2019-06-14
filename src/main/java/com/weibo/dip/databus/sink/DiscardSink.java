package com.weibo.dip.databus.sink;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;

/**
 * Created by yurun on 17/10/23.
 */
public class DiscardSink extends Sink {

    @Override
    public void setConf(Configuration conf) throws Exception {
        this.name = this.getClass().getSimpleName();
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Message message) throws Exception {
        // do nothing
    }

    @Override
    public void stop() {

    }

}

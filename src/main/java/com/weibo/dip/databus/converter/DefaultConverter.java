package com.weibo.dip.databus.converter;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Converter;
import com.weibo.dip.databus.core.Message;

/**
 * Created by yurun on 17/8/31.
 */
public class DefaultConverter extends Converter {

    @Override
    public void setConf(Configuration conf) throws Exception {

    }

    @Override
    public void start() {

    }

    @Override
    public Message convert(Message message) {
        return message;
    }

    @Override
    public void stop() {

    }

}

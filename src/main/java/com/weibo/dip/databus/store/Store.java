package com.weibo.dip.databus.store;

import com.weibo.dip.databus.core.Configurable;
import com.weibo.dip.databus.core.Lifecycle;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;

/**
 * Created by yurun on 17/8/28.
 */
public abstract class Store implements Configurable, Lifecycle {

    protected Sink sink;

    public void link(Sink sink) {
        this.sink = sink;
    }

    public abstract void store(Message message);

}

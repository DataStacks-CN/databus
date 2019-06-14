package com.weibo.dip.databus.core;

/**
 * Created by yurun on 17/8/31.
 */
public abstract class Converter implements Configurable, Lifecycle {

    public abstract Message convert(Message message);

}

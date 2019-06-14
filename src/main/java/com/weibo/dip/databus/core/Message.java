package com.weibo.dip.databus.core;

/**
 * Created by yurun on 17/8/31.
 */
public class Message {

    private String topic;

    private String data;

    public Message() {

    }

    public Message(String topic, String data) {
        this.topic = topic;
        this.data = data;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

}

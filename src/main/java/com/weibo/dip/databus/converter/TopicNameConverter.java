package com.weibo.dip.databus.converter;

import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Converter;
import com.weibo.dip.databus.core.Message;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 17/8/31.
 */
public class TopicNameConverter extends Converter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicNameConverter.class);

    private static final String TOPIC_MAPPINGS = "topic.mappings";

    private Map<String, String> topics = new HashMap<>();

    @Override
    public void setConf(Configuration conf) throws Exception {
        String mappings = conf.get(TOPIC_MAPPINGS);
        if (StringUtils.isEmpty(mappings)) {
            return;
        }

        String[] pairs = mappings.split(Constants.COMMA);

        for (String pair : pairs) {
            String[] words = pair.split(Constants.COLON);
            if (ArrayUtils.isEmpty(words) || words.length != 2) {
                continue;
            }

            topics.put(words[0], words[1]);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Message convert(Message message) {
        String topic = message.getTopic();
        if (Objects.isNull(topic)) {
            return message;
        }

        if (MapUtils.isEmpty(topics) || !topics.containsKey(topic)) {
            return message;
        }

        topic = topics.get(topic);

        message.setTopic(topic);

        return message;
    }

}

package com.weibo.dip.databus.core;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by yurun on 17/8/9.
 */
public class Configuration {

    private Map<String, String> properties = new HashMap<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public Configuration() {

    }

    public Configuration(Properties conf) {
        load(conf);
    }

    public Configuration(File propertyesFile) throws IOException {
        this(new FileInputStream(propertyesFile));
    }

    public Configuration(InputStream in) throws IOException {
        Properties conf = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));

            conf.load(reader);
        } finally {
            if (Objects.nonNull(reader)) {
                reader.close();
            } else {
                if (Objects.nonNull(in)) {
                    in.close();
                }
            }
        }

        load(conf);
    }

    private void load(Properties conf) {
        lock.writeLock().lock();

        try {
            for (String key : conf.stringPropertyNames()) {
                set(key, conf.getProperty(key));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void set(String key, String value) {
        lock.writeLock().lock();

        try {
            properties.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String get(String key) {
        String value;

        lock.readLock().lock();

        try {
            value = properties.get(key);
        } finally {
            lock.readLock().unlock();
        }

        return value;
    }

    public String get(String key, String defaultValue){
        String value = get(key);

        if(value != null){
            return value;
        }

        return defaultValue;
    }

    public Integer getInteger(String key, Integer defaultValue){
        String value = get(key);

        if(value != null){
            return Integer.parseInt(value.trim());
        }

        return defaultValue;
    }

    public boolean getBoolean(String key, boolean defaultValue){
        String value = get(key);

        if(value != null){
            return Boolean.parseBoolean(value.trim());
        }

        return defaultValue;
    }

}

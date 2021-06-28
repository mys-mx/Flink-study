package com.amos.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Title: StartUpClass
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/24 21:59
 */
public class StartUpClass {
    private StreamExecutionEnvironment env = null;
    private Properties properties = null;
    private String topic = "flink-kafka";

    public StartUpClass() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setParallelism(3);
        properties = new Properties();

        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "flink-kafka-002");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset", "latest");
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getTopic() {
        return topic;
    }
}

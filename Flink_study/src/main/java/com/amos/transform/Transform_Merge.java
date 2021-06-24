package com.amos.transform;

import com.amos.source.MyKafkaDeserializationSchema;
import com.amos.source.MyKafkaDeserializationSchema2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import scala.Tuple3;

import java.util.Properties;

/**
 * @Title: Transform_Merge
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/22 20:56
 */
public class Transform_Merge {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "flink-kafka-002");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset", "latest");

        DataStreamSource<Tuple3<String, String, String>> streamSource = env.addSource(new FlinkKafkaConsumer<>("flink-kafka",
                new MyKafkaDeserializationSchema2(), properties));
        streamSource.print();
        env.execute();
    }
}

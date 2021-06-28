package com.amos.sink;

import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @Title: Sink_Kafka
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/25 19:42
 */
public class Sink_Kafka {

    public static void main(String[] args) throws Exception {
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>(
                startUpClass.getTopic(),
                new SimpleStringSchema(),
                startUpClass.getProperties()));

        /*source.addSink(new FlinkKafkaProducer<String>(
                "hadoop01:9092,hadoop02:9092,hadoop03:9092",
                "sinktest",
                new SimpleStringSchema()));*/

        source.addSink(new FlinkKafkaProducer<String>(
                "sink",
                new ProducerStringSerializationSchema("sink"),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        ));
        env.execute();
    }
}

package com.amos.source;

import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Title: Source_kafka
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/20 19:23
 */
public class Source_kafka {
    public static void main(String[] args) throws Exception{

        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStreamSource<String> stringDataStreamSource = env.addSource(
                new FlinkKafkaConsumer<String>(startUpClass.getTopic(),
                        new SimpleStringSchema(),
                        startUpClass.getProperties()));
        stringDataStreamSource.print();
        env.execute();

    }
}

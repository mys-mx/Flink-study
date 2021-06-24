package com.amos.transform;

import com.amos.util.StartUpClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Title: Transform_Base
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/21 18:35
 */
public class Transform_Base {
    public static void main(String[] args) throws Exception {
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStreamSource<String> stringDataStreamSource = env.addSource(
                new FlinkKafkaConsumer<String>(startUpClass.getTopic(),
                new SimpleStringSchema(),
                startUpClass.getProperties()));


        //map 把String转换成长度输出
        stringDataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String words) throws Exception {
                int length = words.split(" ").length;
                return length;
            }
        }).print("map");

        //flatMap 按逗号分字段
        stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String > collector) throws Exception {
                String[] split = s.split(",");
                for (String word:split
                     ) {
                collector.collect(word);

                }


            }
        }).print("flatmap");
        stringDataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.endsWith("0");
            }
        }).print("filter");

        env.execute();
    }
}

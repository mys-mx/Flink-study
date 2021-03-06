package com.amos.transform;

import com.amos.source.MyKafkaDeserializationSchema2;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple3;

import java.util.Collections;
import java.util.Properties;

/**
 * @Title: Transform_union
 * @Description: union可以合并多条流  但是多条流的类型必须一致
 * @Author: YuSong.Mu
 * @Date: 2021/6/24 20:57
 */
public class Transform_union {
    public static void main(String[] args) throws Exception{

        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStreamSource<Tuple3<String,String,String>> streamSource = env.addSource(
                new FlinkKafkaConsumer<>(startUpClass.getTopic(),
                new MyKafkaDeserializationSchema2(),
                startUpClass.getProperties()));

        SplitStream<Tuple2<String, String>> splitStream = streamSource.map(new MapFunction<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple3<String, String, String> value) throws Exception {
                String[] strings = value._3().split("\t");
                return new Tuple2<>(strings[1], strings[3]);
            }
        }).split(new OutputSelector<Tuple2<String, String>>() {
            @Override
            public Iterable<String> select(Tuple2<String, String> value) {
                return Integer.parseInt(value.f1) > 50 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<Tuple2<String, String>> low = splitStream.select("low");
        DataStream<Tuple2<String, String>> high = splitStream.select("high");
        DataStream<Tuple2<String, String>> allSpeed = splitStream.select("low","high");

        DataStream<Tuple2<String, String>> dataStream = low.union(high).union(allSpeed);
        dataStream.print();
        env.execute();
    }
}

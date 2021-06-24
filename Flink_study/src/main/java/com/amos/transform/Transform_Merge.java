package com.amos.transform;

import com.amos.bean.CarSpeedInfo;
import com.amos.source.MyKafkaDeserializationSchema;
import com.amos.source.MyKafkaDeserializationSchema2;
import com.amos.util.StartUpClass;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import scala.Tuple3;

import java.util.Collections;
import java.util.Properties;

/**
 * @Title: Transform_Merge
 * @Description: connect 只能合并两条流，但是两条流的类型可以不一样
 * @Author: YuSong.Mu
 * @Date: 2021/6/22 20:56
 */
public class Transform_Merge {
    public static void main(String[] args) throws Exception {
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStreamSource<Tuple3<String,String,String>> streamSource = env.addSource(
                new FlinkKafkaConsumer<>(startUpClass.getTopic(),
                new MyKafkaDeserializationSchema2(),
                startUpClass.getProperties()));


        //分流操作
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


        //合流  connect

        SingleOutputStreamOperator<CarSpeedInfo> lowBean = low.map(new MapFunction<Tuple2<String, String>, CarSpeedInfo>() {
            @Override
            public CarSpeedInfo map(Tuple2<String, String> value) throws Exception {
                return new CarSpeedInfo(1L, value.f0, "ddd", Integer.parseInt(value.f1));
            }
        });

        ConnectedStreams<Tuple2<String, String>, CarSpeedInfo> connect = high.connect(lowBean);

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> result = connect.map(new CoMapFunction<Tuple2<String, String>, CarSpeedInfo, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map1(Tuple2<String, String> value) throws Exception {
                return new Tuple3<>(value.f0, Integer.parseInt(value.f1), "high speed warning");
            }

            @Override
            public Tuple3<String, Integer, String> map2(CarSpeedInfo value) throws Exception {
                return new Tuple3<>(value.getCarId(), value.getSpeed(), "normal");
            }
        });

        result.print();


        env.execute();
    }
}

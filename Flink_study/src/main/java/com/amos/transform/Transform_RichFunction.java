package com.amos.transform;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Title: Transform_RichFunction
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/24 21:18
 */
public class Transform_RichFunction {
    public static void main(String[] args) throws Exception {
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStreamSource<String> stringDataStreamSource = env.addSource(
                new FlinkKafkaConsumer<String>(startUpClass.getTopic(),
                new SimpleStringSchema(),
                startUpClass.getProperties()));

        DataStream<CarSpeedInfo> streamOperator = stringDataStreamSource.map(line -> {
            String[] strings = line.split("\t");
            return new CarSpeedInfo(Long.parseLong(strings[0]), strings[1], strings[2], Integer.parseInt(strings[3]));
        });

        streamOperator.map(new MyMapper()).print();


        env.execute();

    }

    public static class MyMapper0 implements MapFunction<CarSpeedInfo,Tuple2<String,String>>{

        @Override
        public Tuple2<String, String> map(CarSpeedInfo value) throws Exception {
            return new Tuple2<>(value.getCarId(),value.getSpeed()+"");
        }
    }



    public static class MyMapper extends RichMapFunction<CarSpeedInfo,Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(CarSpeedInfo value) throws Exception {
            return new Tuple2<String,Integer>(value.getCarId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，一般是定义状态，或者建立连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //关闭连接或者清理状态
            System.out.println("close");
        }
    }
}
package com.amos.transform;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Title: Transform_MutiplieStreams
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/22 19:46
 */
public class Transform_MutiplieStreams {
    public static void main(String[] args) throws Exception{
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
        //1.按照车速分流  按照60为界分为两条流
        SplitStream<CarSpeedInfo> splitStream = streamOperator.split(new OutputSelector<CarSpeedInfo>() {
            @Override
            public Iterable<String> select(CarSpeedInfo carSpeedInfo) {
                return (carSpeedInfo.getSpeed() > 60 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });


        DataStream<CarSpeedInfo> highSpeed = splitStream.select("high");
        DataStream<CarSpeedInfo> lowSpeed = splitStream.select("low");
        DataStream<CarSpeedInfo> allSpeed = splitStream.select("high","low");

        highSpeed.print("highSpeed");

        lowSpeed.print("lowSpeed");

        allSpeed.print("allSpeed");
        env.execute();

    }
}

package com.amos.transform;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Title: TransForm_RollingAggregation
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/21 19:44
 */
public class TransForm_RollingAggregation {
    public static void main(String[] args) throws Exception{
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStreamSource<String> stringDataStreamSource = env.addSource(
                new FlinkKafkaConsumer<String>(startUpClass.getTopic(),
                new SimpleStringSchema(),
                startUpClass.getProperties()));
/*        source.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String,Long> map(String s) throws Exception {
                String[] strings = s.split("\t");
                String string = strings[0];
                return new Tuple2<String, Long>(strings[1],Long.parseLong(strings[0]));
            }
        }).keyBy(0).maxBy(1).print();*/


       //lambdas
        stringDataStreamSource.map(line->{
            String[] strings = line.split("\t");
            CarSpeedInfo carSpeedInfo = new CarSpeedInfo(Long.parseLong(strings[0]),strings[1],strings[2],Integer.parseInt(strings[3]));

            return carSpeedInfo;
        }).keyBy("carId").max("speed").print();


       /* source.map(line->{
            String[] strings = line.split("\t");
            CarSpeedInfo carSpeedInfo = new CarSpeedInfo(Long.parseLong(strings[0]),strings[1],strings[2],Integer.parseInt(strings[3]));

            return carSpeedInfo;
        }).keyBy("carId").reduce(new ReduceFunction<CarSpeedInfo>() {
            @Override
            public CarSpeedInfo reduce(CarSpeedInfo value1, CarSpeedInfo value2) throws Exception {

                return new CarSpeedInfo(value1.getMonitorId(),value1.getCarId(),value2.getTimeStamp(),
                        value1.getSpeed()>value2.getSpeed()?value1.getSpeed():value2.getSpeed());
            }
        }).print();*/
        env.execute();

    }
}

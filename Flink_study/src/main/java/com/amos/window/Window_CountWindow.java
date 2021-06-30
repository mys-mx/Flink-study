package com.amos.window;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @Title: Window_CountWindow
 * @Date: 2021/6/28 22:06
 */
public class Window_CountWindow {
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

        //计数窗口函数
        //  增量聚合函数
        //按照滑动窗口步长输出
//        KeyedStream<CarSpeedInfo, Tuple> carId = streamOperator.keyBy("carId");
        streamOperator.keyBy("carId")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

//        result.print();
        env.execute();
    }

    /*
    tuple中第一个元素是车速之和，第二个元素是个数
     */
    private static class MyAvgTemp implements AggregateFunction<CarSpeedInfo, Tuple2<Double, Integer>, Double> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(CarSpeedInfo value, Tuple2<Double, Integer> accumulator) {
            Tuple2<Double, Integer> integerTuple2 = new Tuple2<>(accumulator.f0 + value.getSpeed(), accumulator.f1 + 1);
            return integerTuple2;
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }



}

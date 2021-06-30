package com.amos.window;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * @Title: Window_TimeWindow
 * @Date: 2021/6/28 22:06
 */
public class Window_TimeWindow {
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

        //开窗测试
        // 1. 增量聚合函数
        DataStream<Integer> resultStream = streamOperator.keyBy("carId")
                .timeWindow(Time.seconds(3))//参数一个是滚动窗口，参数两个是滑动窗口
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)));
                .aggregate(new AggregateFunction<CarSpeedInfo, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;//初始值
                    }

                    @Override
                    public Integer add(CarSpeedInfo value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        //2.全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> result = streamOperator.keyBy("carId")
                .timeWindow(Time.seconds(4))
                // IN OUT KEY W
                .apply(new WindowFunction<CarSpeedInfo, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<CarSpeedInfo> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        int size = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(tuple.getField(0),window.getEnd(),size));
                    }
                });

//        result.print("");
        resultStream.print();
        env.execute();
    }
}

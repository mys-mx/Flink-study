package com.amos.state;

import com.amos.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * @Title: StateTest3_KeyedStateApplicationCase
 * @Date: 2021/7/4 19:02
 */
public class StateTest3_KeyedStateApplicationCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置watermark的周期时间
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop01", 7777);
        DataStream<SensorReading> streamOperator = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));

        });

        // 定义一个flatmap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = streamOperator.keyBy("id")
                .flatMap(new MyKeyCountMapper(10.0));

        result.print();


        env.execute();

    }


    private static class MyKeyCountMapper extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>> {
        private Double threshold;

        public MyKeyCountMapper(Double threshold) {
            this.threshold = threshold;
        }

        //定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;
        @Override
        public void open(Configuration parameters) throws Exception {

            lastTempState=getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));

        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            Double temperature = value.getTemperature();
            if(temperature!=null){
                double abs = Math.abs(value.getTimeStamp() - temperature);
                if(abs>=10.0){
                    out.collect(new Tuple3<>(value.getId(),value.getTemperature(),temperature));
                }
            }

            //更新状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}

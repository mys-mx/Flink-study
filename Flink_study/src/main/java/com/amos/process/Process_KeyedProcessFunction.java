package com.amos.process;


import com.amos.bean.SensorReading;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Title: Process_KeyedProcessFunction
 * @Date: 2021/7/5 20:24
 */
public class Process_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop01", 7777);
        DataStream<SensorReading> streamOperator = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));

        });


        //测试keyedProcessFunction，先分组然后自定义处理

        streamOperator.keyBy("id")
                .process(new MyProcess())
                .print();


        env.execute();
    }


    private static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());


            //context
            //当前数据的时间戳
            ctx.timestamp();
            ctx.getCurrentKey();
//            ctx.output();
            //当前处理时间
            ctx.timerService().currentProcessingTime();
            //当前的事件时间
            ctx.timerService().currentWatermark();
            //定时器要触发的时间点的时间戳

            //一秒后触发该时间戳
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
//            ctx.timerService().registerEventTimeTimer((value.getTimeStamp() + 10) * 1000);
            state.update(ctx.timerService().currentProcessingTime() + 1000L);
            //删除闹钟
//            ctx.timerService().deleteProcessingTimeTimer(state.value());
//            ctx.timerService().deleteEventTimeTimer((value.getTimeStamp() + 10) * 1000);
        }

        /**
         *触发时间之后要做的动作
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {

            System.out.println(timestamp + "====定时器触发");
        }

        @Override
        public void close() throws Exception {
            state.clear();
        }
    }
}

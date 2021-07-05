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
 * @Title: Process_ApplictionCase
 * @Date: 2021/7/5 21:16
 */
public class Process_ApplictionCase {
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
                .process(new TempConsIncreaWaring(10))
                .print();

        env.execute();
    }

    /**
     * 实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
     */
    private static class TempConsIncreaWaring extends KeyedProcessFunction<Tuple, SensorReading, String> {
        private Integer interval;

        public TempConsIncreaWaring(Integer interval) {
            this.interval = interval;
        }

        //定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();


            //如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                //计算出定时器时间戳
                long ts = ctx.timerService().currentProcessingTime() + interval + 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                //更新状态
                timerTsState.update(ts);

            }//如果温度下降删除定时器
            else if(value.getTemperature()<=lastTemp && timerTs!=null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            //更新温度状态
            lastTempState.update(value.getTemperature());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //定时器触发,输出报警信息
            out.collect("传感器"+ctx.getCurrentKey().getField(0)+"温度值连续"+interval+"s上升");

            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}

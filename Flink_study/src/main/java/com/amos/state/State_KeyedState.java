package com.amos.state;

import com.amos.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @Title: State_KeyedState
 * @Date: 2021/7/4 19:02
 */
public class State_KeyedState {

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

        //定义一个有状态的map操作，统计当前sensor分区数据个数
        SingleOutputStreamOperator<Integer> result = streamOperator.keyBy("id")
                .map(new MyKeyCountMapper());

        result.print();


        env.execute();

    }


    private static class MyKeyCountMapper extends  RichMapFunction<SensorReading,Integer> {
        private ValueState<Integer> keyCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}

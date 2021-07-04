package com.amos.state;

import com.amos.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @Title: State_OperatorState
 * @Date: 2021/7/4 19:02
 */
public class State_OperatorState {

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

        //定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> result = streamOperator.map(new MyCountMapper());

        result.print();


        env.execute();

    }

    private static class MyCountMapper implements MapFunction<SensorReading, Integer> ,ListCheckpointed<Integer> {
        //定义一个本地变量，作为算子状态
        private Integer count = 0;


        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {

            for(Integer num:state)
                count+=num;
        }
    }
}

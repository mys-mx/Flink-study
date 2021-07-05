package com.amos.process;

import com.amos.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Title: Process_SideOutputCase
 * @Date: 2021/7/5 21:16
 */
public class Process_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop01", 7777);
        DataStream<SensorReading> streamOperator = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));

        });

        //定义一个outputtag用来表示测输出流
        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp") {
        };

        //测试ProcessFunction，自定义测输出流实现分流操作

        SingleOutputStreamOperator<SensorReading> highTempStream = streamOperator
                .process(new ProcessFunction<SensorReading, SensorReading>() {


                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        //判断温度，大于30度，高温流输出到主流；小于30度，低温流输出到测输出流
                        if (value.getTemperature() > 30) {
                            out.collect(value);
                        } else {
                            ctx.output(lowTemp, value);
                        }

                    }
                });
        highTempStream.print("high");
        DataStream<SensorReading> lowTempStream = highTempStream.getSideOutput(lowTemp);
        lowTempStream.print("low");


        env.execute();
    }


}

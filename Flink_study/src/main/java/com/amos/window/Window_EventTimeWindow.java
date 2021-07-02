package com.amos.window;

import com.amos.bean.CarSpeedInfo;
import com.amos.bean.SensorReading;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.io.OutputStream;

/**
 * @Title: Window_EventTimeWindow
 * @Date: 2021/6/30 21:32
 */
public class Window_EventTimeWindow {
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

        })      /*
                    在assignTimestampsAndWatermarks中要做两件事：
                        1.提取时间戳
                        2.生成watermark
                */
                .assignTimestampsAndWatermarks(
                        //参数为 maxOutOfOrderness 最大的乱序时间 -->两秒的延迟  -->乱序数据设置最大延迟时间
                        //周期性生成watermark
                        new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                            @Override //提取时间戳
                            public long extractTimestamp(SensorReading element) {
                                //返回毫秒数
                                return element.getTimeStamp() * 1000L;
                            }
                        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        //基于事件时间的开窗聚合--->统计15秒内的温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = streamOperator.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("min temperature");
        minTempStream.getSideOutput(outputTag).print("late");
        env.execute();
    }
}

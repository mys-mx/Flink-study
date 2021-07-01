package com.amos.window;

import com.amos.bean.CarSpeedInfo;
import com.amos.bean.SensorReading;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @Title: Window_EventTimeWindow
 * @Date: 2021/6/30 21:32
 */
public class Window_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置watermark的周期时间
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop01", 7777);
        DataStream<SensorReading> streamOperator = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));

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
                                return element.getTimeStamp() * 1000;
                            }
                        });


        env.execute();
    }
}

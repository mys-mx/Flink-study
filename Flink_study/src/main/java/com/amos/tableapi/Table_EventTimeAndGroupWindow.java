package com.amos.tableapi;

import com.amos.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @program: Flink_study
 * @description: 基于事件时间语义窗口的滚动窗口
 * @create: 2021-07-10 19:30
 */
public class Table_EventTimeAndGroupWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //创建Table环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> stringDataStreamSource =
                env.readTextFile("D:\\FLINK\\Flink-study\\Flink_study\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimeStamp() * 1000L;
                    }
                });

        //将流转换成表，定义时间特性 法一： rowtime --> 事件时间
        Table dataTable = tableEnv
                .fromDataStream(dataStream, "id,timeStamp as ts,temperature as temp,rt.rowtime");
        dataTable.printSchema();

        //窗口操作
        //1. group window
        // table api

        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");

        //sql
        tableEnv.createTemporaryView("sensor", dataTable);

        Table sqlResultTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp,tumble_end(rt,interval '10' second) " +
                "from sensor group by id, tumble(rt,interval '10' second)");

        //结果输出
//        tableEnv.toAppendStream(dataTable, Row.class).print();
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(sqlResultTable, Row.class).print("sqlResult");

        env.execute();
    }

}

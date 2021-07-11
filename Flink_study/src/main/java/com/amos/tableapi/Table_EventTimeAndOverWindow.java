package com.amos.tableapi;

import com.amos.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @program: Flink_study
 * @description: 基于事件时间语义窗口的table Api
 * @create: 2021-07-10 19:30
 */
public class Table_EventTimeAndOverWindow {
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
//        tableEnv.toAppendStream(dataTable, Row.class).print();


        //table API
        OverWindowedTable overWindowedTable = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"));
        Table overResult = overWindowedTable.select("id,rt,id.count over ow,temp.avg over ow");


        // SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        Table overSQLResult = tableEnv.sqlQuery("select id,count(id) over ow,avg(temp) over ow,rt from sensor " +
                "window ow as (partition by id order by rt rows between 2 preceding and current row)");
        Table overSQLResult1 = tableEnv.sqlQuery("select id,count(id) over(partition by id order by rt rows between 2 preceding and current row) cn" +
                ",avg(temp) over(partition by id order by rt rows between 2 preceding and current row) an,rt from sensor ");

//        tableEnv.toRetractStream(overResult,Row.class).print("overResult");
        tableEnv.toRetractStream(overSQLResult,Row.class).print("Result");
        tableEnv.toRetractStream(overSQLResult1,Row.class).print("Result1");
        env.execute();
    }

}

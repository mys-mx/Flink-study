package com.amos.tableapi;

import com.amos.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @program: Flink_study
 * @description:flinkTable Api
 * @create: 2021-07-08 19:06
 */
public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.读取数据
        DataStream<String> stringDataStreamSource = env.socketTextStream("hadoop01", 7777);


        DataStream<SensorReading> map = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        //3.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4,基于流创建一张表
        Table table = tableEnv.fromDataStream(map);

        //5.调用tableApi进行转换操作
        Table resultTable = table.select("id,temperature")
                .where("id='sensor_1'");

        //6.执行sql
        tableEnv.registerTable("sensor", table);
        String sql = "select id,temperature from sensor where id='sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);



        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSql");


        env.execute();

    }
}

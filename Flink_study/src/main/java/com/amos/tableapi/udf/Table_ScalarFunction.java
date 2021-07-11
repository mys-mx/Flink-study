package com.amos.tableapi.udf;

import com.amos.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @program: Flink_study
 * @description: 用户自定义函数（udf）:用户定义函数必须先注册，然后才能在查询中使用
 * 函数通过registerFunction()方法在TableEnvironment中注册，当用户定义的函数被注册时，
 * 它被插入到TableEnvironment的函数目录中，这样Table API 或 SQL解析器就可以识别并正确地解释它
 * <p>
 * 1.scalar function （标量函数）：用户定义得标量函数，可以将0、1或多个标量值映射到新的标量值
 * 为了定义标量函数，必须继承ScalarFunction函数，并实现(一个或多个)求值(eval)方法
 * @create: 2021-07-10 19:30
 */
public class Table_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //创建Table环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取数据
        DataStream<String> stringDataStreamSource =
                env.readTextFile("D:\\FLINK\\Flink-study\\Flink_study\\src\\main\\resources\\sensor.txt");

        //转换成pojo
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

        //将流转换成表
        Table dataTable = tableEnv
                .fromDataStream(dataStream, "id,timeStamp as ts,temperature as temp,rt.rowtime");
        dataTable.printSchema();

        //自定义标量函数，实现求id的hash值
        HashCode hashCode=new HashCode(23);

        //需要在环境中进行注册udf
        tableEnv.registerFunction("hashCode",hashCode);

        Table resultTable= dataTable.select("id,ts,hashCode(id)");

        //SQL 先注册一张表
        tableEnv.createTemporaryView("sensor",dataTable);
        Table sqlQuery = tableEnv.sqlQuery("select id,ts,hashCode(id) from sensor");

        tableEnv.toRetractStream(resultTable,Row.class).print("result");
        tableEnv.toRetractStream(sqlQuery,Row.class).print("SQL");


        env.execute();
    }

    //实现自定义的ScalarFunction
    public static class HashCode extends ScalarFunction {
        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
            return str.hashCode() * factor;
        }
    }

}

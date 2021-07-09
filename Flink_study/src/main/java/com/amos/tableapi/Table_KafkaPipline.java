package com.amos.tableapi;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import java.util.Properties;

/**
 * @program: Flink_study
 * @description: 读取kafka数据 并 输出到kafka
 * @create: 2021-07-08 20:22
 */
public class Table_KafkaPipline {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //连接kafka
        StreamTableDescriptor connect = tableEnv.connect(new Kafka()
                .topic("sensor1")
                .version("universal")
                .property("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
                .property("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092"));



        //读取kafka数据
        connect.withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");


        //建立kafka连接，输出到不同的topic下
        connect.withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");


        //从inputTable中将数据抽取出
        Table inputTable = tableEnv.from("inputTable");


        // 转换
        Table filterTable = inputTable.select("id,temperature")
                .filter("id==='sensor_6'");



        //数据写出到kafka
        filterTable.insertInto("outputTable");

        env.execute();

    }
}

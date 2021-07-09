package com.amos.tableapi;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @program: Flink_study
 * @description:
 * @create: 2021-07-08 20:22
 */
public class Table_TransformApi {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.表的创建：连接外部系统，读取数据
        String filePath = "D:\\FLINK\\Flink-study\\Flink_study\\src\\main\\resources\\sensor.txt";

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timesteamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        Table inputTable = tableEnv.from("inputTable");

        //1.table Api
        // 简单转换
        Table filterTable = inputTable.select("id,temp")
                .filter("id==='sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as cnt,temp.avg as avg");

        //2. SQL
        Table sqlWhereTable = tableEnv.sqlQuery("select id,temp from inputTable where id='sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) cnt,avg(temp) tempAvg from inputTable group by id");


        tableEnv.toAppendStream(filterTable,Row.class).print("filter");
        tableEnv.toRetractStream(aggTable,Row.class).print("agg1");
        tableEnv.toRetractStream(sqlWhereTable,Row.class).print("where");
        tableEnv.toRetractStream(sqlAggTable,Row.class).print("agg2");


        env.execute();

    }
}

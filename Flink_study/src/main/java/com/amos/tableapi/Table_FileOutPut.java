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
 * @description:输出到文件
 * @create: 2021-07-08 20:22
 */
public class Table_FileOutPut {
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

        //输出到文件
        //1.连接外部文件注册输出表
        String outPutPath = "D:\\FLINK\\Flink-study\\Flink_study\\src\\main\\resources\\output.txt";

        tableEnv.connect(new FileSystem().path(outPutPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        //写入到文件中
        filterTable.insertInto("outputTable");
        //TODO 聚合类的操作不能写入到文件中


        env.execute();

    }
}

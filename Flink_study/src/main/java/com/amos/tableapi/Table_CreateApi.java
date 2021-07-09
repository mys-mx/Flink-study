package com.amos.tableapi;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

/**
 * @program: Flink_study
 * @description:
 * @create: 2021-07-08 20:22
 */
public class Table_CreateApi {
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

        inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print("table");


        env.execute();

    }
}

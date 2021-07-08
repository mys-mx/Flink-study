package com.amos.tableapi;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @program: Flink_study
 * @description:
 * @create: 2021-07-08 20:22
 */
public class Table_CommonApi {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1基于老版本的planner的流处理
        EnvironmentSettings oldStreamSetting = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSetting);

        //1.2基于blink planner的批处理
        EnvironmentSettings batchBuild = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment batchTable = TableEnvironment.create(batchBuild);

        //1.2 基于blink的planner的流处理
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSetting);

        env.execute();

    }
}

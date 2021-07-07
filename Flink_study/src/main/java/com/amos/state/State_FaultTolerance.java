package com.amos.state;
/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.state
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/11 9:30
 */


import com.amos.bean.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StateTest4_FaultTolerance
 * @Description: 状态后端配置和检查点配置
 * @Version: 1.0
 */
public class State_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2. 检查点配置
        /**
         * 不配置的情况下默认是500毫秒进行一次checkpoint
         */
        env.enableCheckpointing(300);

        // 高级选项
        //状态一致性的配置 EXACTLY_ONCE：精确一次  AT_LEAST_ONCE：至少一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /**
         * checkpoint超时时间：超过1分钟就丢弃
         */
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        /**
         * 最大同时执行的checkpoint有两个
         */
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        /**
         * 两段checkpoint之间的间歇时间不小于100毫秒：目的是防止资源都在处理checkpoint，间歇时间就是flink在处理真正的数据
         * 设置间歇时间之后同时执行的checkpoint只能有一个
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);

        /**
         * false：不管是checkpoint还是savepoint ，flink只找最近的存盘点进行恢复
         */
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        /**
         * 容忍checkpoint失败多少次：0就是不容忍checkpoint失败，就是checkpoint挂了就是任务挂了
         */
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 3. 重启策略配置：就是在任务挂掉之后在checkpoint中如何重启任务
        /**
         * 固定延迟重启：3-->尝试重启三次  10000-->每隔10000毫秒重启一次
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        /**
         * 失败率重启 : 3-->尝试重启三次  10-->10分钟内统计重启次数   1-->每隔1分钟重启一次
         */
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("hadoop01", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();
        env.execute();
    }
}

package com.amos.sink;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/**
 * @Title: Sink_Kafka
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/25 19:42
 */
public class Sink_redis {

    public static void main(String[] args) throws Exception {
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStream<String> source = env.addSource(new FlinkKafkaConsumer<String>(
                startUpClass.getTopic(),
                new SimpleStringSchema(),
                startUpClass.getProperties()));

        DataStream<CarSpeedInfo> streamOperator = source.map(line -> {
            String[] strings = line.split("\t");
            return new CarSpeedInfo(Long.parseLong(strings[0]), strings[1], strings[2], Integer.parseInt(strings[3]));
        });

        //定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop01")
                .setPort(6379)
                .setPassword("123456")
                .build();

        streamOperator.addSink(new RedisSink<>(config,
                new MyRedisMapper()));

        env.execute();
    }

    //自定义redisMapper
    public static class MyRedisMapper implements RedisMapper<CarSpeedInfo> {
        //定义保存数据到redis的命令，存成hash表，hset sensor_speed
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_speed");
        }

        @Override
        public String getKeyFromData(CarSpeedInfo carSpeedInfo) {
            return carSpeedInfo.getCarId();
        }

        @Override
        public String getValueFromData(CarSpeedInfo carSpeedInfo) {
            return carSpeedInfo.getSpeed() + "";
        }
    }
}

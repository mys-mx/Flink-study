package com.amos.source;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Title: MyKafkaDeserializationSchema
 * @Description: 自定义source 带出mate信息
 * @Author: YuSong.Mu
 * @Date: 2021/6/23 21:12
 */
public class MyKafkaDeserializationSchema2 implements KafkaDeserializationSchema<Tuple3<String,String,String>> {


    @Override
    public boolean isEndOfStream(Tuple3<String, String, String> nextElement) {
        return false;
    }

    @Override
    public Tuple3<String, String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        long offset = record.offset();
        String topic = record.topic();
        String key = new String(record.key(), StandardCharsets.UTF_8);
        String value = new String(record.value(), StandardCharsets.UTF_8);
        return new Tuple3<String,String,String>(topic,key,value);
    }

    @Override
    public TypeInformation<Tuple3<String, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {});
    }
}

package com.amos.source;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple3;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @Title: MyKafkaDeserializationSchema
 * @Description: 自定义source 带出mate信息
 * @Author: YuSong.Mu
 * @Date: 2021/6/23 21:12
 */
public class MyKafkaDeserializationSchema implements KeyedDeserializationSchema<Tuple3<String,String,String>> {


    @Override
    public Tuple3<String, String, String> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {

        String key=null;
        String value=null;
        if(messageKey!=null){
            key=new String(messageKey,StandardCharsets.UTF_8);
        }
        if(message!=null){
            value=new String(message,StandardCharsets.UTF_8);
        }
        return new Tuple3<String,String,String>(topic,key,value);
    }

    @Override
    public boolean isEndOfStream(Tuple3<String, String, String> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Tuple3<String, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {});
    }
}

package com.amos.sink;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @Title: ProducerStringSerializationSchema
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/25 22:40
 */
public class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {
    private String topic;

    public ProducerStringSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
    }


}

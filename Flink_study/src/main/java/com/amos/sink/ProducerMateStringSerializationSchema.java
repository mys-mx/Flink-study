package com.amos.sink;

import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * @Title: ProducerStringSerializationSchema
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/25 22:40
 */
public class ProducerMateStringSerializationSchema implements KafkaContextAware<String> {


    @Override
    public void setParallelInstanceId(int parallelInstanceId) {



    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {

    }

    @Override
    public void setPartitions(int[] partitions) {

    }

    @Override
    public String getTargetTopic(String element) {
        return null;
    }
}

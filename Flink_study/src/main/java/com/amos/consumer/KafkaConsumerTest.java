package com.amos.consumer;


import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @Title: KafkaConsumerTest
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/23 20:30
 */
public class KafkaConsumerTest implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private final String topic;
    private static final String GROUPID = "groupA";


    public KafkaConsumerTest(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("group.id", GROUPID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<String, String>(props);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        int messageNo = 1;
        System.out.println("---------开始消费---------");
        try {
            for (;;) {
                msgList = consumer.poll(1000);
                if(null!=msgList&&msgList.count()>0){
                    for (ConsumerRecord<String, String> record : msgList) {
                        //消费100条就打印 ,但打印的数据不一定是这个规律的
                        if(messageNo%100==0){
                            System.out.println(messageNo+"=======receive: key = " + record.key() + ", value = " + record.value()+" offset==="+record.offset());
                        }
                        //当消费了1000条就退出
                        if(messageNo%1000==0){
                            break;
                        }
                        messageNo++;
                    }
                }else{
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
    public static void main(String args[]) {
        KafkaConsumerTest test1 = new KafkaConsumerTest("flink-kafka");
        Thread thread1 = new Thread(test1);
        thread1.start();
    }
}
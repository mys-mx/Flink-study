package com.amos.stream

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Title: ReadKafka
  * Description: 读取kafka中数据  key value全部读出来
  * Author: YuSong.Mu
  * Date: 2021/5/20 10:11
  */
object ReadKafkaConsumer {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置连接kafka的配置信息
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("group.id", "flink-kafka-001")
    properties.setProperty("key.deserializer", classOf[StringSerializer].getName)
    properties.setProperty("value.deserializer", classOf[StringSerializer].getName)

    //消费kafka中的key和value  ---->(String, String)
    val value = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", new KafkaDeserializationSchema[(String, String)] {
      //什么时候停止
      override def isEndOfStream(t: (String, String)): Boolean = {
        false
      }

      //反序列化的字节流
      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(consumerRecord.key(), "UTF-8")
        val value = new String(consumerRecord.value(), "UTF-8")
        (key, value)
      }

      //指定下一个返回的数据类型，Flink提供的类型
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, properties))
    value.print()

    env.execute()

  }
}

package com.amos.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

/**
  * Title: KafkaProducer
  * Description: java类作用描述
  * Author: YuSong.Mu
  * Date: 2021/5/20 12:00
  */
object FlinkKafkaProducer {
  def main(args: Array[String]): Unit = {

    //配置连接kafka的信息
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    //创建一个生产者对象
    val producer = new KafkaProducer[String, String](properties)

    val iterator = Source.fromFile("./data/carFlow_all_column_test.txt").getLines()


    for (i <- 1 to 100) {
      for (elem <- iterator) {
        val arr: Array[String] = elem.split(",")
        val monitorId = arr(0).replace("'", "")
        val carId = arr(2).replace("'", "")
        val timeStamp = arr(4).replace("'", "")
        val speed = arr(6).replace("'", "")


        val builder = new StringBuilder
        val info: StringBuilder = builder.append(monitorId + "\t").append(carId + "\t").append(timeStamp + "\t").append(speed)

        Thread.sleep(300)
        val str = info.toString()
        producer.send(new ProducerRecord[String, String]("flink-kafka", i + "", str))

      }
    }

  }

}

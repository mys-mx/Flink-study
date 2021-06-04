package com.amos.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * Title: ReadHDFS
  * Description: java类作用描述
  * Author: YuSong.Mu
  * Date: 2021/5/18 23:47
  */
object ReadHDFS {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[String] = env.readTextFile("hdfs://hadoop01:9000/flink/data/wc")

    value.print()


    env.execute()

  }

}

package com.amos.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * Title: ReadCollections
  * Description: java类作用描述
  * Author: YuSong.Mu
  * Date: 2021/5/20 10:08
  */
object ReadCollections {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[Int] = env.fromCollection(List(1,2,3,4,5))

    stream.print()

    env.execute()

  }

}

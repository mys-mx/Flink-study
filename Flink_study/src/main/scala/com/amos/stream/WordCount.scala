package com.amos.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Title: WordCount
  * Description: java类作用描述
  * Author: YuSong.Mu
  * Date: 2021/5/17 14:11
  */

/**
  * 相同的数据一定是由某一个thread处理
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    //准备环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局线程数
    env.setParallelism(1)

    val initStream: DataStream[String] = env.socketTextStream("hadoop01", 8888)

    val flatMapTask = initStream.flatMap(_.split(" ")).setParallelism(2).startNewChain()
    val mapTask = flatMapTask.map((_, 1)).setParallelism(3)
    val keyByTask = mapTask.keyBy(0)
    val result: DataStream[(String, Int)] = keyByTask.sum(1).setParallelism(2)

    result.print().setParallelism(1)

    //启动flink
    env.execute("first flink job")
  }

}

package com.amos.stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Title: StreamReadHDFS
  * Description: java类作用描述
  * Author: YuSong.Mu
  * Date: 2021/5/20 9:48
  */
object StreamReadHDFS {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val filePath = "hdfs://hadoop01:9000/flink/data/wc"
    val textInputFormat = new TextInputFormat(new Path(filePath))


    try {
      val textInputStream: DataStream[String] = env.readFile(textInputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
      textInputStream.flatMap(t => t.split(" ")).map((_, 1)).keyBy(_._1).sum(1).print()
    } catch {
      case e: Exception =>e.printStackTrace()
    }


    env.execute()

  }

}

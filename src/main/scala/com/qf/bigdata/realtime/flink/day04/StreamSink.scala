package com.qf.bigdata.realtime.flink.day04

import com.qf.bigdata.realtime.flink.until.FlinkHelper
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

object StreamSink {

  val LOG : Logger = LoggerFactory.getLogger("")

  def sink2FileSystem(fsPath:String): Unit ={

    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
    senv.setParallelism(2)

    val datas = Seq[String]("java","go","python","go","java","java")
    val wordDS:DataStream[(String,Int)] = senv.fromCollection(datas)
        .map(word =>(word,1))
        .keyBy(0)
        .sum(1)
//        .print("test")

    wordDS.writeAsCsv(fsPath)

    senv.execute()
  }





  def main(args: Array[String]): Unit = {

    sink2FileSystem("file:///E:/data/Flink/")
  }
}

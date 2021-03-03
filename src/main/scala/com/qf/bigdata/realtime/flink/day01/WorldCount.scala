package com.qf.bigdata.realtime.flink.day01

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

/**
  * flink 单词统计
  */
object WorldCount {

  /**
    * 日志
    */
  val LOG : Logger = LoggerFactory.getLogger("")


  def demo(input:String):Unit = {
    try{
      //1 Flink运行环境构建
      val senv = StreamExecutionEnvironment.getExecutionEnvironment
      //senv.setParallelism(1)

      //2 数据源&数据处理
//      val ds:DataStream[String] = senv.socketTextStream("192.168.44.206",9999, '\n')

      import org.apache.flink.api.scala._

      val ds:DataStream[String] = senv.readTextFile(input)
      //3 统计结果输出
      ds.flatMap(_.split("\\s+|,+"))
        .map((_, 1))
        .keyBy(0)
        .sum(1)
        .print()

      //4 任务执行
      senv.execute("Flink WorldCount")
    }catch {
      case ex:Exception =>{
        LOG.error("Flink.worldcount.err=>", ex)
      }
    }


  }

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input")

//    val input = "D:\\Test\\words.txt"

    demo(input)
  }

}

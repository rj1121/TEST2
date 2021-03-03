package com.qf.bigdata.realtime.flink.until

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkHelper {

  //构建执行环境
  def createStreamEnv(): StreamExecutionEnvironment ={
    val senv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    senv
  }

}

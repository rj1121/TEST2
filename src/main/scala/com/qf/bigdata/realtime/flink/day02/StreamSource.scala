package com.qf.bigdata.realtime.flink.day02

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.day01.WorldCount
import com.qf.bigdata.realtime.flink.day02.StreamSourceFun.{JDBCSource, RandomSource}
import com.qf.bigdata.realtime.flink.until.StreamCC.{temp_classify, test1}
import com.qf.bigdata.realtime.flink.util.{GsonUtil, PropertyUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object StreamSource {

  val LOG : Logger = LoggerFactory.getLogger("")



  def useRandomSource():Unit = {

    val senv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sleep = 1000 * 1
    val randomSource = new RandomSource(sleep)

    val randomCharsDS = senv.addSource(randomSource)
        .map((_,1))
        .keyBy(0)
        .sum(1)


    randomCharsDS.print("StreamSource.randomCharsDS")

    senv.execute("StreamSource.useRandomSource")
  }

  def useJDBCSource():Unit = {
    //1 构建上下文执行环境
    val senv :StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2 读取数据源
    val sql = "select * from test1"

    val jdbcPro:Properties = PropertyUtil.readProperties(QRealTimeConstant.JDBC_CONFIG_URL)


    val richSource = new JDBCSource(jdbcPro,sql)

    //4 输出
    val recordsDS:DataStream[Row] = senv.addSource(richSource)

    val ds:DataStream[test1] = recordsDS.map(
      (row:Row) => {
        val id = row.getField(0).toString.toInt
//        val min_temp = row.getField(1).toString.toInt
//        val max_temp = row.getField(2).toString.toInt
//        val classify = row.getField(3).toString
        val name = row.getField(1).toString


        test1(id,name)
//        temp_classify(id,min_temp,max_temp,classify)

      }
    )

    ds.print("StreamSource.useJDBCSource========")



    //5 任务执行
    senv.execute("useJDBCSource")
  }





  def main(args: Array[String]): Unit = {

//    useRandomSource()


    useJDBCSource()
  }

}

package com.qf.bigdata.realtime.flink.day02

import com.qf.bigdata.realtime.flink.until.FlinkHelper
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable

object StreamInput {

  val LOG : Logger = LoggerFactory.getLogger("")

  def useJDBCSource():Unit = {

    //1 构建上下文环境
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()

    //2 读取数据源
    val schema:mutable.ListBuffer[TypeInformation[_]] = new mutable.ListBuffer[TypeInformation[_]]()


//    new mutable.ListBuffer[TypeInformation[_]]()



    schema.+=(BasicTypeInfo.INT_TYPE_INFO)
    schema.+=(BasicTypeInfo.STRING_TYPE_INFO)

    val typeInfo = new RowTypeInfo(schema:_*)

    val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setAutoCommit(true)
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://172.18.16.207:3306/mysql?user=UTC&characterEncoding=utf-8")
      .setUsername("root")
      .setPassword("123456")
      .setQuery("select * from test1")
      .setRowTypeInfo(typeInfo)
      .finish()

    val ds = senv.createInput(jdbcInputFormat)

    ds.print("useJDBCSource")

    senv.execute("useJDBCSource")
  }

  def jdbcRead(env: ExecutionEnvironment) ={
    val inputMysql: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      //    .指定驱动名称
      .setDrivername("com.mysql.jdbc.Driver")
      //      url
      .setDBUrl("jdbc:mysql://172.18.16.207:3306/mysql")
      .setUsername("root")
      .setPassword("123456")
      .setQuery("select * from test1;")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .finish()
    )
    inputMysql
  }

  def main(args: Array[String]): Unit = {
    useJDBCSource()

//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val inputMysql: DataSet[Row] = jdbcRead(env)
//
//    inputMysql.print()
  }

}

package com.qf.bigdata.realtime.flink.day02

import java.sql._
import java.util.{Properties, Random}

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object StreamSourceFun {

  val LOG : Logger = LoggerFactory.getLogger("")

  class RandomSource(sleep:Long) extends SourceFunction[String]{

    var isRUN = true
    var count:Int = 0
    val rangeChars = "abcdefghijkmnopqwxyz"

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

      val rangeCharsLength = rangeChars.length
      while(isRUN){
        val randomNum = new Random().nextInt(rangeCharsLength) % rangeCharsLength
        val randomChar = String.valueOf(rangeChars.charAt(randomNum))
        ctx.collect(randomChar)
        count += 1
        println(s"RandomSource.char.count=>${count}")
        Thread.sleep(sleep)
      }
    }

    override def cancel(): Unit = {
      isRUN = false
      println(s"RandomSource.cancel")
    }
  }


  class JDBCSource(jdbcPro:Properties,sql:String) extends RichSourceFunction[Row]{


    var conn: Connection = _
    var ps:PreparedStatement = _
    var rs:ResultSet = _
    var isRunning = true

    override def open(parameters: Configuration): Unit = {
      try{

        val driver = jdbcPro.getProperty(QRealTimeConstant.JDBC_DRIVER)
        val url = jdbcPro.getProperty(QRealTimeConstant.JDBC_URL)
        val username = jdbcPro.getProperty(QRealTimeConstant.JDBC_USERNAME)
        val passwd = jdbcPro.getProperty(QRealTimeConstant.JDBC_PASSWD)

        Class.forName(driver)
        conn = DriverManager.getConnection(url,username,passwd)
        ps = conn.prepareStatement(sql)

      }catch{
        case ex:Exception => println(ex.getMessage)
      }

    }

    override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
      rs = ps.executeQuery(sql)
      val colValues = new mutable.ArrayBuffer[java.lang.Object]()
      while(isRunning && rs.next()){
        val rsMetaData:ResultSetMetaData = rs.getMetaData
        val colCount = rsMetaData.getColumnCount

        for(i <- 1 to colCount){
          val colName = rsMetaData.getCatalogName(i)
          val colValue = rs.getObject(i)

          colValues.+=(colValue)

        }

        val record = Row.of(colValues:_*)
        colValues.clear()

        ctx.collect(record)

      }
    }

    override def close(): Unit = {
      if(rs !=null){
        rs.close()
      }
      if(ps !=null){
        ps.close()
      }
      if(conn !=null){
        conn.close()
      }


    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }



}

package com.qf.bigdata.realtime.flink.day09

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.{Person, WindowPerson}
import com.qf.bigdata.realtime.flink.util.{CommonUtil, GsonUtil, PropertyUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

object StreamWindowBasic {


  val LOG : Logger = LoggerFactory.getLogger("")

  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()
    tim
  }

  def useWindow4Socket(): Unit ={

    val senv = FlinkHelper.createStreamEnv()

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.enableCheckpointing(500)
//    println(s"now==>${CommonUtil.formatDate4Def(new Date())}")

    val windowPersonDS:DataStream[WindowPerson] = senv.socketTextStream("192.168.44.206",6666)
        .map(files =>{
          val file = files.split(",")
          val country:String = file(0)
          val name:String = file(1)
          val ct:Long = tranTimeToLong(file(2))
          val salary:Long = file(3).toLong

          WindowPerson(country,name,ct,salary)
        })

    windowPersonDS.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks)
        .keyBy(_.country)
        .timeWindow(Time.seconds(10))
        .allowedLateness(Time.seconds(5))
        .aggregate(new RJAggFunc(),new RJWinFunc())
        .print("useWindow4Socket")


    senv.execute("useWindow4Socket")
  }


  def useWindow4kafka(topic:String): Unit ={

    val senv = FlinkHelper.createStreamEnv()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.enableCheckpointing(500)

    val props = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new SimpleStringSchema()

    //连接kafka
    val kafkaConsumer:FlinkKafkaConsumer[String] =  new FlinkKafkaConsumer[String](topic,schema,props)
    kafkaConsumer.setStartFromLatest()

    val ds:DataStream[String] = senv.addSource(kafkaConsumer)

    val windowPersonDS:DataStream[WindowPerson] = ds.map(files =>{
      val file = files.split(",")
      val country:String = file(0)
      val name:String = file(1)
      val ct:Long = tranTimeToLong(file(2))
      val salary:Long = file(3).toLong

      WindowPerson(country,name,ct,salary)
    })


    windowPersonDS.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks)
      .keyBy(_.country)
      .timeWindow(Time.seconds(10))
//      .allowedLateness(Time.seconds(5))
      .aggregate(new RJAggFunc(),new RJWinFunc())
      .print("useWindow4Kafka")


    senv.execute()

  }

  def main(args: Array[String]): Unit = {

    val topic:String = "t_window"
//    useWindow4Socket()

    useWindow4kafka(topic)
  }

}

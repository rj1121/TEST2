package com.qf.bigdata.realtime.flink.day10

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.day09.{MyAssignerWithPeriodicWatermarks, RJAggFunc, RJWinFunc}
import com.qf.bigdata.realtime.flink.day09.StreamWindowBasic.tranTimeToLong
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.WindowPerson
import com.qf.bigdata.realtime.flink.util.PropertyUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.scala._
object StreamWindowSession {

  def useWindow4kafka2Session(topic:String,internal:Int): Unit ={

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
        .window(EventTimeSessionWindows.withGap(Time.seconds(internal)))
        .sum("salary")
//      .print("useWindow4kafka2Session")


    senv.execute()

  }

  def main(args: Array[String]): Unit = {

    val topic:String = "t_window"


  }

}

package com.qf.bigdata.realtime.flink.day10

import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.day04.KafkaDSerdePersonSchema
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import com.qf.bigdata.realtime.flink.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object StreamTrigger {

  /**
    * 基于kafka数据源+时间触发器的window操作
    * @param topic
    * @param windowSize
    * @param maxInternal
    * @param offset
    */

  def useWindow4kafka2TimeTrigger(topic:String,windowSize:Long,maxInternal:Long,offset:Long): Unit ={

    val senv = FlinkHelper.createStreamEnv()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.enableCheckpointing(500)

    val props = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new KafkaDSerdePersonSchema()

    //连接kafka
    val kafkaConsumer:FlinkKafkaConsumer[Person] =  new FlinkKafkaConsumer[Person](topic,schema,props)
    kafkaConsumer.setStartFromLatest()

    senv.addSource(kafkaConsumer)
        .assignTimestampsAndWatermarks(new QPAssignerWithPeriodicWatermarks())
        .keyBy(_.age)
        .window(TumblingEventTimeWindows.of(Time.hours(windowSize)))
        .trigger(new RJProcessTimeTrigger(maxInternal,offset,TimeUnit.SECONDS))
        .max("temp")
        .print("===============>")


    senv.execute("useWindow4kafka2TimeTrigger")

  }

  def main(args: Array[String]): Unit = {

    val topic:String = "t_tmp_201"
    val windowSize:Long = 1
    val maxInternal:Long = 10
    val offset:Long = 2

    useWindow4kafka2TimeTrigger(topic,windowSize,maxInternal,offset)
  }

}

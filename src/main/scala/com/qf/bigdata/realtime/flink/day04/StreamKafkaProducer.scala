package com.qf.bigdata.realtime.flink.day04

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.util.PropertyUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object StreamKafkaProducer {

  val LOG : Logger = LoggerFactory.getLogger("")


  def useKafkaProducer(): Unit ={
    val senv = FlinkHelper.createStreamEnv()

    val props = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)


    val schema = new SimpleStringSchema()

    //连接kafka
//    val kafkaConsumer:FlinkKafkaConsumer[String] =  new FlinkKafkaConsumer[String](topic,schema,props)
//    kafkaConsumer.setStartFromLatest()


//    senv.addSource(kafkaConsumer).addSink()

    senv.execute("useKafkaProducer")
  }



  def main(args: Array[String]): Unit = {

  }

}

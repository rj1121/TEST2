package com.qf.bigdata.realtime.flink.day08


import java.net.URI

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import com.qf.bigdata.realtime.flink.util.{CommonUtil, GsonUtil, PropertyUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
object CheckpointDemo {
  val LOG : Logger = LoggerFactory.getLogger("")
  def useCheckpoint():Unit = {
    //1 构建上下文执行环境
    val senv :StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
    senv.enableCheckpointing(1000*5,CheckpointingMode.EXACTLY_ONCE)

    //2 读取数据源
    val datas = new mutable.ListBuffer[Person]()
    for(idx <- 1 to 100){
      val name:String = CommonUtil.getRandom(1)
      val age:Int = CommonUtil.getRandomNum(2)
      val temp:Int = CommonUtil.getRandomNum(2)
      val ct:Long = CommonUtil.getRandomTimestamp
      val p = new Person(name, age, temp, ct)
      datas.+=(p)
    }
    val ds:DataStream[Person] = senv.fromCollection(datas)

    ds.rescale.print("rescale").setParallelism(4)


    senv.execute("useCheckpoint")


  }

  def useKafkaConsumer(topic:String): Unit ={

    val senv = FlinkHelper.createStreamEnv()
    senv.setParallelism(1)
    senv.enableCheckpointing(1000*5,CheckpointingMode.EXACTLY_ONCE)

    val checkpoiontDatauri:String = "file:///E:/data/Flink/checkpoint"
    senv.setStateBackend(new RocksDBStateBackend(checkpoiontDatauri:String))
    val props = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)


    val schema = new SimpleStringSchema()

    //连接kafka
    val kafkaConsumer:FlinkKafkaConsumer[String] =  new FlinkKafkaConsumer[String](topic,schema,props)
    kafkaConsumer.setStartFromLatest()

    val ds:DataStream[String] = senv.addSource(kafkaConsumer)
    val personDS:DataStream[Person] = ds.map(GsonUtil.gObject2Json(_,classOf[Person]))

    personDS.print("personDS")
    senv.execute()

  }


  def main(args: Array[String]): Unit = {

    val topic:String = "t_tmp_201"
//    useCheckpoint()

    useKafkaConsumer(topic)
  }

}

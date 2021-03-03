package com.qf.bigdata.realtime.flink.day13

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object Data_Analysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val aData: DataStream[AD] = env.readTextFile("source/AC.txt")
      .map(fields => {
        val line = fields.split(",")
        val uid = line(0).trim
        val deviceID = line(1).trim
        AD(uid, deviceID)
      })


    env.readTextFile("source/DD.txt")
      .map(fields => {
        val line = fields.split(",", -1).filter(_ != null)
        val deviceID = line(0).trim
        val dt = line(1).trim.toInt
        if (line(2).isEmpty) {
          line(2) = "220"
        }
        val deviceData = line(2).trim.toInt
        DD(deviceID, dt, deviceData)
      })
      .keyBy(_.deviceID)
      .flatMap(new MyFunction)
      .print()

    env.execute("Data_Analysis")
  }
}

class MyFunction extends  RichFlatMapFunction[DD,(String,String,String,Int)] {
  var sum:ValueState[(String,String,Int)] = _

  override def open(parameters: Configuration): Unit = {


    //定义值状态描述器
    val sumed: ValueStateDescriptor[(String,String,Int)] = new ValueStateDescriptor[(String,String,Int)](
      "sumed",
      TypeInformation.of(new TypeHint[(String,String,Int)] {}),
      ("","",0)
    )


    //获取状态
    sum = getRuntimeContext().getState(sumed)
  }
  override def flatMap(value: DD, out: Collector[(String, String , String, Int)]): Unit ={
    val current_value:(String,String,Int) = sum.value()
    var startTime = current_value._1
    var stopTime = current_value._2
    var state = current_value._3
    if(value.deviceData == 220){
      state = 0
    }else if(value.deviceData > 220 ){
      state += 1
    }
    if(value.deviceData > 220 && state == 0){
      startTime = value.dt.toString
    }else if(current_value._3 != 0 && state ==0) {
      stopTime = (value.dt - 1).toString
      state = current_value._3
    }
    sum.update(startTime,stopTime,state)
    out.collect(value.deviceID,startTime,stopTime,state)
    if(value.deviceData == 220){
      sum.clear()
    }
  }



  override def close(): Unit = super.close()
}

case class AD(uid:String, deviceID:String)
case class DD(deviceID:String, dt:Int,deviceData:Int)
case class AbnormalInformation(uid:String,deviceID:String,startTime:Int,stopTime:Int,DurationTime:Int)
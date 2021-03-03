package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.{PersonWideClassify, Temp_Last10Minute_Mid}
import org.apache.flink.api.common.functions.AggregateFunction

class Test1AggFunc extends AggregateFunction[PersonWideClassify,Temp_Last10Minute_Mid,Temp_Last10Minute_Mid]{
  override def createAccumulator(): Temp_Last10Minute_Mid = {
    Temp_Last10Minute_Mid(0,0,0,0)
  }

  override def add(value: PersonWideClassify, accumulator: Temp_Last10Minute_Mid): Temp_Last10Minute_Mid = {

    val sum = value.temp + accumulator.sum
    val count = accumulator.count + 1L
    val maxTemp = value.temp.max(accumulator.maxTemp)
    val avrTemp = sum/count

    Temp_Last10Minute_Mid(sum,count,maxTemp,avrTemp)
  }



  override def merge(a: Temp_Last10Minute_Mid, b: Temp_Last10Minute_Mid): Temp_Last10Minute_Mid = {

    val sum = a.sum + b.sum
    val count = a.count + b.count
    val maxTemp = Math.max(a.maxTemp,b.maxTemp)
    val avrTemp = sum/count
    Temp_Last10Minute_Mid(sum,count,maxTemp,avrTemp)
  }


  override def getResult(accumulator: Temp_Last10Minute_Mid): Temp_Last10Minute_Mid = {
    accumulator
  }
}

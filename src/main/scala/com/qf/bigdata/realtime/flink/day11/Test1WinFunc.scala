package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.{Temp_Last10Minute, Temp_Last10Minute_Mid}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class Test1WinFunc extends WindowFunction[Temp_Last10Minute_Mid,Temp_Last10Minute,String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Temp_Last10Minute_Mid],
                     out: Collector[Temp_Last10Minute]): Unit ={

    val begin = window.getStart
    val end = window.getEnd
    var count = 0L
    var sum = 0L
    var maxTemp = 0
    var avrTemp = 0L

    for(rt <- input){
      count += rt.count
      sum += rt.sum
      maxTemp = rt.maxTemp.max(maxTemp)
    }
    avrTemp = sum/count
    val record = new Temp_Last10Minute(begin,end,count,maxTemp,avrTemp)
    out.collect(record)
  }

}

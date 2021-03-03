package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.{Temp_Every10000Person, Temp_Every10000Person_Mid}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

class Test3WinFunc extends WindowFunction[Temp_Every10000Person_Mid,Temp_Every10000Person,String,GlobalWindow]{
  override def apply(key: String,
                     window: GlobalWindow,
                     input: Iterable[Temp_Every10000Person_Mid],
                     out: Collector[Temp_Every10000Person]): Unit = {


    var count:Long = 0L

    for(rt <- input){
      count += rt.count
    }


    val record = Temp_Every10000Person(key,count)
    out.collect(record)
  }
}

package com.qf.bigdata.realtime.flink.day11

import java.util

import com.qf.bigdata.realtime.flink.until.StreamCC.{PersonWideClassify, UnusualTemp_Every10Minute, UnusualTemp_Every10Minute_Mid}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class Test2WinFunc extends WindowFunction[UnusualTemp_Every10Minute_Mid,UnusualTemp_Every10Minute,String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[UnusualTemp_Every10Minute_Mid],
                     out: Collector[UnusualTemp_Every10Minute]): Unit = {

    val begin = window.getStart
    val end = window.getEnd
    var persons:util.ArrayList[PersonWideClassify] = new util.ArrayList[PersonWideClassify]()



    for (rt <- input){
      persons.addAll(rt.persons)
    }

    val record = UnusualTemp_Every10Minute(begin,end,persons)
    out.collect(record)
  }
}

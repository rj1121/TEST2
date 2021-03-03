package com.qf.bigdata.realtime.flink.day09

import com.qf.bigdata.realtime.flink.until.StreamCC.{WindowPersonDimMeas, WindowPersonMid}
import org.apache.flink.streaming.api.scala.function.{ WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class RJWinFunc extends WindowFunction[WindowPersonMid,WindowPersonDimMeas,String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[WindowPersonMid],
                     out: Collector[WindowPersonDimMeas]): Unit = {

    var maxSalary = 0L
    var totalCount = 0L
    for(pm <- input){
      maxSalary = pm.maxSalary.max(maxSalary)

      totalCount += pm.totalCount
    }

    val begin = window.getStart
    val end = window.getEnd
    val record = new WindowPersonDimMeas(key,maxSalary,totalCount,begin,end)
    out.collect(record)
  }
}

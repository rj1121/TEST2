package com.qf.bigdata.realtime.flink.day09

import com.qf.bigdata.realtime.flink.until.StreamCC.{ WindowPerson}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class MyAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[WindowPerson]{

  var lastEventTime:Long = 0L



  override def getCurrentWatermark: Watermark = {

    new Watermark(lastEventTime)
  }



  override def extractTimestamp(element: WindowPerson,
                                previousElementTimestamp: Long): Long = {

    val ct = element.ct
    lastEventTime = Math.max(lastEventTime,ct)
    ct
  }
}

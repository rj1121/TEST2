package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class RJAssignerWithPeriodicWatermarks(maxLateness:Long = 0L) extends AssignerWithPeriodicWatermarks[Person]{
  var lastEventTime = 0L

  override def getCurrentWatermark: Watermark = {

    new Watermark(lastEventTime - maxLateness)
  }

  override def extractTimestamp(element: Person,
                                previousElementTimestamp: Long): Long = {

    val ct = element.ct
    lastEventTime = Math.max(lastEventTime,ct)
    ct
  }
}

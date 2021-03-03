package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.{Person, PersonWideClassify}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class TestAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[PersonWideClassify]{

  var lastEventTime:Long = 0L
  override def getCurrentWatermark: Watermark = {
    new Watermark(lastEventTime)
  }

  override def extractTimestamp(element: PersonWideClassify, previousElementTimestamp: Long): Long = {
    val ct = element.ct
    lastEventTime = Math.max(lastEventTime,ct)
    ct
  }
}

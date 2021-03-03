package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.PersonWideClassify
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TestTrigger(maxSum:Long) extends Trigger[PersonWideClassify,TimeWindow]{



  //当前执行数量状态描述及状态对象
  val CURRENT_SUM_NAME = "CURRENT_SUM"
  val CURRENT_SUM_DESC:ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](CURRENT_SUM_NAME,createTypeInformation[Long])
  var CURRENT_SUM :ValueState[Long] = _

  override def onElement(element: PersonWideClassify,
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {



    CURRENT_SUM = ctx.getPartitionedState(CURRENT_SUM_DESC)

    if(CURRENT_SUM == null){
      CURRENT_SUM.update(0L)
    }else{
      CURRENT_SUM.update(CURRENT_SUM.value() + 1L)
    }

    if(CURRENT_SUM.value() >= maxSum) {
      TriggerResult.FIRE
    }

    TriggerResult.CONTINUE

  }

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

    ctx.getPartitionedState(CURRENT_SUM_DESC).clear()
  }
}

package com.qf.bigdata.realtime.flink.day10

import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.concurrent.duration.TimeUnit
/**
  * 基于处理时间的自定义触发器
  * 需求：现有1小时长窗口
  * （1）每10分钟输出一次中间结果
  * （2）最终结果也要输出
  *
  */


class RJProcessTimeTrigger(maxInternal:Long,offset:Long,timeUnit:TimeUnit) extends Trigger[Person,TimeWindow]{


  //触发小周期（10分钟）
  val maxInternalTimes = Time.of(maxInternal,timeUnit).toMilliseconds

  //偏移长度
  val offsetTimes:Long = Time.of(offset, timeUnit).toMilliseconds

  //执行处理时间状态描述及状态对象
  val NEXT_PROCESS_TIME_NAME = "NEXT_PROCESS_TIME"
  val NEXT_PROCESS_TIME_DESC:ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](NEXT_PROCESS_TIME_NAME,createTypeInformation[Long])
  var NEXT_PROCESS_TIME :ValueState[Long] = _




  def computeNextProcessTime(curProcessTime:Long): Long ={


//    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
//      return timestamp - (timestamp - offset + windowSize) % windowSize;
//    }

    val nextStartTime:Long = TimeWindow.getWindowStartWithOffset(curProcessTime,offsetTimes,maxInternalTimes)

    //下次计算的开始时间
    //下次执行时间，设置到定时器中
    val nextEndTime = nextStartTime + maxInternalTimes


    nextEndTime
  }



  /**
    * 对窗口中每一个element进行处理
    */
  override def onElement(element: Person,
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {

    //维护状态值，处理时间
    NEXT_PROCESS_TIME = ctx.getPartitionedState(NEXT_PROCESS_TIME_DESC)
    if(null == NEXT_PROCESS_TIME.value()){

      //当前处理时间
      val curProcessTime:Long = ctx.getCurrentProcessingTime

      //下次触发时间
      val nextProcessTime = computeNextProcessTime(curProcessTime)

      //注册下次计算时间 ->时间定时器
      ctx.registerProcessingTimeTimer(nextProcessTime)

      //维护状态
      NEXT_PROCESS_TIME.update(nextProcessTime)
    }

    TriggerResult.CONTINUE

  }

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    //维护状态：处理时间
    NEXT_PROCESS_TIME = ctx.getPartitionedState(NEXT_PROCESS_TIME_DESC)
    val curProcessTime:Long = NEXT_PROCESS_TIME.value()

    //窗口结束时间
    val winEnd = window.getEnd

    var result = TriggerResult.CONTINUE

    //如果计时器启动的时间（time）== 上一次设置好的触发器时间
    if(curProcessTime == time){

      //设置下次触发时间
      val nextProcesstime = computeNextProcessTime(curProcessTime)

      //注册下次计算时间
      ctx.registerProcessingTimeTimer(nextProcesstime)

      //维护状态
      NEXT_PROCESS_TIME.update(nextProcesstime)
      result = TriggerResult.FIRE

      //（time）计时器启动时间
    }else if(time >= winEnd){
      NEXT_PROCESS_TIME.clear()

      result = TriggerResult.FIRE
    }

    result
  }

  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow,
                     ctx: Trigger.TriggerContext): Unit = {

    NEXT_PROCESS_TIME = ctx.getPartitionedState(NEXT_PROCESS_TIME_DESC)

    //清除状态数据
    NEXT_PROCESS_TIME.clear()

    //清除上次定时器
    ctx.deleteProcessingTimeTimer(NEXT_PROCESS_TIME.value())

  }
}

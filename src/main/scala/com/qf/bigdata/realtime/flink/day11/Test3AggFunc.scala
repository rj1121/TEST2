package com.qf.bigdata.realtime.flink.day11

import com.qf.bigdata.realtime.flink.until.StreamCC.{PersonWideClassify, Temp_Every10000Person_Mid}
import org.apache.flink.api.common.functions.AggregateFunction

class Test3AggFunc extends AggregateFunction[PersonWideClassify,Temp_Every10000Person_Mid,Temp_Every10000Person_Mid]{
  override def createAccumulator(): Temp_Every10000Person_Mid = {
    Temp_Every10000Person_Mid(0L)
  }

  override def add(value: PersonWideClassify,
                   accumulator: Temp_Every10000Person_Mid): Temp_Every10000Person_Mid = {

    val count = accumulator.count + 1L
    Temp_Every10000Person_Mid(count)
  }

  override def getResult(accumulator: Temp_Every10000Person_Mid): Temp_Every10000Person_Mid = {
    accumulator
  }

  override def merge(a: Temp_Every10000Person_Mid, b: Temp_Every10000Person_Mid): Temp_Every10000Person_Mid = {

    Temp_Every10000Person_Mid(a.count + b.count)

  }
}

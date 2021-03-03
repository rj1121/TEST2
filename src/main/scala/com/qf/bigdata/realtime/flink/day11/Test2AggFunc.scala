package com.qf.bigdata.realtime.flink.day11

import java.util

import com.qf.bigdata.realtime.flink.until.StreamCC.{PersonWideClassify, UnusualTemp_Every10Minute_Mid}
import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable

class Test2AggFunc extends AggregateFunction[PersonWideClassify,UnusualTemp_Every10Minute_Mid,UnusualTemp_Every10Minute_Mid]{
  override def createAccumulator(): UnusualTemp_Every10Minute_Mid = {
    UnusualTemp_Every10Minute_Mid(new util.ArrayList[PersonWideClassify]())
  }

  override def add(value: PersonWideClassify,
                   accumulator: UnusualTemp_Every10Minute_Mid): UnusualTemp_Every10Minute_Mid = {

    accumulator.persons.add(value)
    accumulator
  }

  override def getResult(accumulator: UnusualTemp_Every10Minute_Mid): UnusualTemp_Every10Minute_Mid = {
    accumulator
  }

  override def merge(a: UnusualTemp_Every10Minute_Mid, b: UnusualTemp_Every10Minute_Mid): UnusualTemp_Every10Minute_Mid = {
    var result:UnusualTemp_Every10Minute_Mid = UnusualTemp_Every10Minute_Mid(new util.ArrayList[PersonWideClassify]())
    result.persons.addAll(a.persons)
    result.persons.addAll(b.persons)
    result
  }
}

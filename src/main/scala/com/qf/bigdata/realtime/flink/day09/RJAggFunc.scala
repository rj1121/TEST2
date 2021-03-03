package com.qf.bigdata.realtime.flink.day09

import com.qf.bigdata.realtime.flink.until.StreamCC.{WindowPerson, WindowPersonMid}
import org.apache.flink.api.common.functions.AggregateFunction

class RJAggFunc extends AggregateFunction[WindowPerson,WindowPersonMid,WindowPersonMid]{
  override def createAccumulator(): WindowPersonMid = {
    WindowPersonMid(0L,0L)
  }

  override def add(value: WindowPerson, accumulator: WindowPersonMid): WindowPersonMid = {

    val maxSalary:Long = value.salary.max(accumulator.maxSalary)
    val totalCount = accumulator.totalCount + 1L
    WindowPersonMid(maxSalary,totalCount )
  }



  override def merge(a: WindowPersonMid, b: WindowPersonMid): WindowPersonMid = {

    val maxSalary = a.maxSalary.max(b.maxSalary)
    val totalCount = a.totalCount + b.totalCount
    WindowPersonMid(maxSalary,totalCount)
  }

  override def getResult(accumulator: WindowPersonMid): WindowPersonMid = {

    accumulator
  }
}

package com.qf.bigdata.realtime.flink.until

import java.util

import scala.collection.mutable

object StreamCC {


  case class Person(name:String,age:Int,temp:Int,ct:Long)
  case class PersonAgg(name:String,minAge:Int,maxAge:Int,count:Long)

  //WaterMark用例
  case class WindowPerson(country:String,name:String,ct:Long,salary:Long)
  case class WindowPersonMid(maxSalary:Long, totalCount:Long)
  case class WindowPersonDimMeas(country:String, maxSalary:Long, totalCount:Long, begin:Long, end:Long)



  case class PersonOffset(name:String,age:Int,temp:Int,ct:Long,topic:String,partition:Int,offset:Long)
  case class KafkaOffset(topic:String,partition:Int,offset:Long,ct:Long)



  //测验schema
  case class RealTimeAll(age_range:String,pv:Int,uv:Int,max_temp:Int)
  case class RealTimeSpecific(hour:String,sum:Int,max_temp:Int)


  //自建宽表
  case class PersonWide(name:String,age:Int,temp:Int,ct:Long,age_range:String,format_ct:String)


  //
  //最近10分钟的检测温度情况
  case class Temp_Last10Minute(begin:Long,end:Long,count:Long,maxTemp:Int,avrTemp:Long)
  case class Temp_Last10Minute_Mid(sum:Long,count:Long,maxTemp:Int,avrTemp:Long)


  //每10分钟的异常温度情况
  case class UnusualTemp_Every10Minute(begin:Long,end:Long,persons: util.ArrayList[PersonWideClassify])
  case class UnusualTemp_Every10Minute_Mid(persons: util.ArrayList[PersonWideClassify])


  //每10000人的人员测温情况
  case class Temp_Every10000Person(tempType:String,count:Long)
  case class Temp_Every10000Person_Mid(count:Long)

  //Mysql表

  case class temp_classify(id:String,min_temp:Int,max_temp:Int,classify:String)
  case class test1(id:Int,name:String)


  //第二个宽表

  case class PersonWideClassify(name:String,age:Int,temp:Int,ct:Long,classify:String)

}

package com.qf.bigdata.realtime.flink.day11

import java.util
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import com.qf.bigdata.realtime.flink.util.CommonUtil
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable
import org.apache.flink.api.scala._
import scala.collection.JavaConversions._
object  StreamCEP {


  def useCEP(): Unit ={

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)

    val sleep = 1000 * 1

    val datas = new mutable.ListBuffer[Person]()
    for(idx <- 1 to 100){
      val name:String = CommonUtil.getRandom(1)
      val age:Int = CommonUtil.getRandomNum(2)
      val temp:Int = CommonUtil.getRandomNum(2)
      val ct:Long = CommonUtil.getRandomTimestamp
      val p = new Person(name, age, temp, ct)
      datas.+=(p)
    }
    val ds:DataStream[Person] = senv.fromCollection(datas)

//    ds.print()
    //规则定义
    val PATTERN_BEGIN = "CEP_NAME"
    val PATTERN_NEXT = "CEP_AGE"

    val pattern:Pattern[Person,Person] = Pattern
        .begin[Person](PATTERN_BEGIN)
        .where(
          _.name.matches("[a-c]")
        )
        .timesOrMore(2)//两次以上
        .consecutive()//连续匹配方式
        .within(Time.minutes(3))//时间范围
        .next(PATTERN_NEXT) //严格连续
        .where(
          (p:Person,ctx) => {
            val age:Int = p.age
            age > 18 || age < 100
          }
        ).times(1)
        .within(Time.of(1,TimeUnit.SECONDS))





    CEP.pattern(ds,pattern).process(

      new PatternProcessFunction[Person,Person]{
        override def processMatch(`match`: util.Map[String, util.List[Person]],
                                  ctx: PatternProcessFunction.Context,
                                  out: Collector[Person]): Unit = {

          val defMatchs = new util.ArrayList[Person]()
          val matchDatas:util.List[Person] = `match`.get(PATTERN_BEGIN)

          if(!matchDatas.isEmpty){
            for(data:Person <- matchDatas.iterator()){
              out.collect(data)
            }
          }

        }
      }
    ).print("CEP==========>result")


    senv.execute("useCEP")

  }

  def main(args: Array[String]): Unit = {

    useCEP()
  }

}

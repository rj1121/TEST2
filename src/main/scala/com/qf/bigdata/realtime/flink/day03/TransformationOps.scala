package com.qf.bigdata.realtime.flink.day03

import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.{Person, PersonAgg}
import com.qf.bigdata.realtime.flink.util.{CommonUtil, GsonUtil}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
object TransformationOps {
  val LOG : Logger = LoggerFactory.getLogger("")

  def useMap():Unit = {

    //1 构建上下文执行环境
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
    senv.setParallelism(2)

    //2 读取数据源
    val datas:ListBuffer[Person] = new mutable.ListBuffer[Person]()
    for (idx <- 1 to 100){
      val name:String = CommonUtil.getRandom(2)
      val age:Int = CommonUtil.getRandomNum(2)
      val temp:Int = CommonUtil.getRandomNum(2)
      val ct:Long = CommonUtil.getRandomTimestamp
      val p = new Person(name, age, temp, ct)
      datas.+=(p)

    }

    val ds:DataStream[Person] = senv.fromCollection(datas)

    val ds2:DataStream[String] = ds.startNewChain()
      .filter(_.age > 10)
      .map(
        GsonUtil.gObject2Json(_)
      )

    ds2.print()

//    val ds3:DataStream[(Person,Int)] = ds.map((_,1))
//    ds3.print()

    senv.execute("useMap")

  }

  def useUnion(): Unit ={
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
    senv.setParallelism(2)

    val datas1:ListBuffer[Person] = new mutable.ListBuffer[Person]()
    val datas2:ListBuffer[Person] = new mutable.ListBuffer[Person]()

    for(idx <- 1 to 4){
      val name:String = CommonUtil.getRandom(2)
      val age:Int = CommonUtil.getRandomNum(2)
      val temp:Int = CommonUtil.getRandomNum(2)
      val ct:Long = CommonUtil.getRandomTimestamp
      val p = new Person(name, age, temp, ct)

      if(idx == 4){
        datas2.+=(p)
      }

      if(idx % 2 == 0){
        datas1.+=(p)
      }else{
        datas2.+=(p)
      }

    }
    val ds1:DataStream[Person] = senv.fromCollection(datas1)
    ds1.print("ds1")
    val ds2:DataStream[Person] = senv.fromCollection(datas2)
    ds2.print("ds2")

    val ds:DataStream[Person] = ds1.union(ds2)

    ds.print("ds")

    senv.execute("useUnion")

  }
  def useSideout(): Unit ={
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
    senv.setParallelism(2)

    val datas = new mutable.ListBuffer[Person]()
    for(idx <- 1 to 5){
      val name:String = CommonUtil.getRandom(2)
      val age:Int = CommonUtil.getRandomNum(2)
      val temp:Int = CommonUtil.getRandomNum(2)
      val ct:Long = CommonUtil.getRandomTimestamp
      val p = new Person(name, age, temp, ct)
      datas.+=(p)
    }

    val ds:DataStream[Person] = senv.fromCollection(datas)

    val outputTag:OutputTag[Person] = new OutputTag[Person]("OldPerson")
    val ErrorTempTag:OutputTag[Person] = new OutputTag[Person]("ErrorTemp")


    val commonDS:DataStream[Person] = ds.process(
      new ProcessFunction[Person, Person]{
        override def processElement(value: Person,
                                    ctx: ProcessFunction[Person, Person]#Context,
                                    out: Collector[Person]): Unit = {
          val record = value

          ctx.timestamp()
          if (value.age >= 60){
            ctx.output(outputTag,value)
          }
          if(value.temp > 37){
            ctx.output(ErrorTempTag,value)
          }

          out.collect(record)
        }
      }

    )

    commonDS.print("commonDS")


    //老年人
    val oldDS:DataStream[Person] = commonDS.getSideOutput[Person](outputTag)

    //体温异常人员
    val ErrorTempDS:DataStream[Person] = commonDS.getSideOutput[Person](ErrorTempTag)

    oldDS.print("oldDS")

    ErrorTempDS.print("ErrorTempDS")

    senv.execute()
  }



  def usrAgg(): Unit ={

    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
    senv.setParallelism(3)


    //读数据
    val datas = new mutable.ListBuffer[Person]

    for(idx <- 1 to 100){
      val name:String = CommonUtil.getRandom(1)
      val age:Int = CommonUtil.getRandomNum(2)
      val temp:Int = CommonUtil.getRandomNum(2)
      val ct:Long = CommonUtil.getRandomTimestamp
      val p = new Person(name, age, temp, ct)
      datas.+=(p)
    }
    val ds:DataStream[Person] = senv.fromCollection(datas)
//    ds.keyBy(_.name).sum("age").print()


    ds.keyBy(_.name)
        .process(
          new KeyedProcessFunction[String,Person,PersonAgg]{


            val max = 0
            val min = 0
            var count = 0

            override def processElement(value: Person,
                                        ctx: KeyedProcessFunction[String, Person, PersonAgg]#Context,
                                        out: Collector[PersonAgg]): Unit = {

              val name = value.name
              val curAge = value.age

              val maxAge = curAge.max(max)
              val minAge = curAge.min(min)

              count += 1
              val pagg = new PersonAgg(name,maxAge,minAge,count)

              out.collect(pagg)
            }
          }
        ).print("useAgg")


    senv.execute()

  }

  def useConnect(): Unit ={

    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()

    val datas1 = new mutable.ListBuffer[(String,Int)]
    datas1.+=(("a",1))
    datas1.+=(("b",1))


    val datas2 = new mutable.ListBuffer[(String,String)]
    datas2.+=(("a","001"))
    datas2.+=(("b","001"))


    val ds1:DataStream[(String,Int)] = senv.fromCollection(datas1)
    val ds2:DataStream[(String,String)] = senv.fromCollection(datas2)


    ds1.connect(ds2).map(

      new CoMapFunction[(String,Int),(String,String),(String,Int,String)]{
        override def map1(value: (String, Int)): (String, Int, String) = {
          (value._1,value._2,"nothing")
        }

        override def map2(value: (String, String)): (String, Int, String) = {
          (value._1,-1,value._2)
        }
      }
    ).print("useConnect")



    senv.execute()

  }






  def main(args: Array[String]): Unit = {

    useMap()

//    useUnion()

//    useSideout()


//    usrAgg()

//    useConnect()



  }


}

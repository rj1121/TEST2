package com.qf.bigdata.realtime.flink.day11

import java.util
import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.day02.StreamSourceFun.JDBCSource
import com.qf.bigdata.realtime.flink.day04.KafkaDSerdePersonSchema
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC._
import com.qf.bigdata.realtime.flink.util.PropertyUtil
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

object PersonTempTest_2 {



  def useWindow4kafka2TimeTrigger(topic:String,maxSum:Long = 10): Unit ={

    val senv = FlinkHelper.createStreamEnv()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    senv.enableCheckpointing(500)

    //从mysql拿到规则
    val sql = "select * from dim_product"
    val jdbcPro:Properties = PropertyUtil.readProperties(QRealTimeConstant.JDBC_CONFIG_URL)
    val richSource = new JDBCSource(jdbcPro,sql)
    val recordsDS:DataStream[Row] = senv.addSource(richSource)
    val jdbcDS:DataStream[temp_classify] = recordsDS.map(
      (row:Row) => {
        val id = row.getField(0).toString
        val min_temp = row.getField(1).toString.toInt
        val max_temp = row.getField(2).toString.toInt
        val classify = row.getField(3).toString

        temp_classify(id,min_temp,max_temp,classify)
      }
    )

    val props = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new KafkaDSerdePersonSchema()

    //连接kafka
    val kafkaConsumer:FlinkKafkaConsumer[Person] =  new FlinkKafkaConsumer[Person](topic,schema,props)
    kafkaConsumer.setStartFromLatest()

    val kafka2SourceDS:DataStream[Person] = senv.addSource(kafkaConsumer)

    val name = "BC_CLASSIFY"
    val mysqlBCDesc:MapStateDescriptor[String,temp_classify] = new MapStateDescriptor[String,temp_classify](name,TypeInformation.of(classOf[String]),TypeInformation.of(classOf[temp_classify]))

//    val mysqlBCDesc = new ValueStateDescriptor[temp_classify](name,TypeInformation.of(classOf[temp_classify]))

    val mysqlDS = jdbcDS.broadcast(mysqlBCDesc)


    val withClassifyDS:DataStream[PersonWideClassify] = kafka2SourceDS.connect(mysqlDS).process(
      new BroadcastProcessFunction[Person,temp_classify,PersonWideClassify]{
        override def processElement(value: Person,
                                    ctx: BroadcastProcessFunction[Person, temp_classify, PersonWideClassify]#ReadOnlyContext,
                                    out: Collector[PersonWideClassify]): Unit = {
          val name = value.name
          val age = value.age
          val ct = value.ct
          val temp = value.temp
          var classify:String = null

          val readonlyMysql:ReadOnlyBroadcastState[String,temp_classify] = ctx.getBroadcastState(mysqlBCDesc)

          val iterator_test = readonlyMysql.immutableEntries()


          val result:Array[temp_classify] = Array(readonlyMysql.get("1"),readonlyMysql.get("2"),readonlyMysql.get("3"),readonlyMysql.get("4"))
          for(i <- result) {

            if(temp >= i.min_temp && temp < i.max_temp){
              classify = i.classify
            }
          }
          val record = new PersonWideClassify(name,age,temp,ct,classify)
          out.collect(record)
        }

        override def processBroadcastElement(value: temp_classify,
                                             ctx: BroadcastProcessFunction[Person, temp_classify, PersonWideClassify]#Context,
                                             out: Collector[PersonWideClassify]): Unit = {
          val context = ctx.getBroadcastState(mysqlBCDesc)

          context.put(value.id,value)
        }
      }
    )


    val test1DS:DataStream[Temp_Last10Minute] = withClassifyDS.assignTimestampsAndWatermarks(new TestAssignerWithPeriodicWatermarks)
        .keyBy(_.classify)
        .timeWindow(Time.minutes(10),Time.minutes(1))
        .aggregate(new Test1AggFunc,new Test1WinFunc)


//    val test2DS = withClassifyDS.assignTimestampsAndWatermarks(new TestAssignerWithPeriodicWatermarks)
//      .filter(_.classify != "01")
//        .keyBy(_.classify)
//        .timeWindow(Time.seconds(10))
//        .aggregate(new Test2AggFunc,new Test2WinFunc)



//    val test3DS = withClassifyDS.assignTimestampsAndWatermarks(new TestAssignerWithPeriodicWatermarks)
//        .keyBy(_.classify)
////        .trigger(new TestTrigger(maxSum))
//        .countWindow(10)
//        .aggregate(new Test3AggFunc,new Test3WinFunc)


    val PATTERN_BEGIN = "TEST2_4"

    val pattern:Pattern[PersonWideClassify,PersonWideClassify] = Pattern.begin[PersonWideClassify](PATTERN_BEGIN)
        .where(
          _.temp >= 37
        )
        .timesOrMore(5)
        .within(Time.minutes(10))


//    val test4DS:DataStream[PersonWideClassify] = CEP.pattern(withClassifyDS,pattern).process(
//      new PatternProcessFunction[PersonWideClassify,PersonWideClassify]{
//        override def processMatch(`match`: util.Map[String, util.List[PersonWideClassify]],
//                                  ctx: PatternProcessFunction.Context,
//                                  out: Collector[PersonWideClassify]): Unit = {
//
//          val matcheDatas:util.List[PersonWideClassify] = `match`.get(PATTERN_BEGIN)
//
//          if(!matcheDatas.isEmpty){
//
//            for(rt:PersonWideClassify <- matcheDatas.iterator()){
//              out.collect(rt)
//            }
//          }
//        }
//      }
//    )


    senv.execute("useWindow4kafka2TimeTrigger")

  }




  def main(args: Array[String]): Unit = {



    useWindow4kafka2TimeTrigger("t_tmp_201")


  }











  def useJDBCSource():Unit = {
    //1 构建上下文执行环境
    val senv :StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2 读取数据源
    val sql = "select * from temp_classify"

    val jdbcPro:Properties = PropertyUtil.readProperties(QRealTimeConstant.JDBC_CONFIG_URL)


    val richSource = new JDBCSource(jdbcPro,sql)

    //4 输出
    val recordsDS:DataStream[Row] = senv.addSource(richSource)

    val jdbcDS:DataStream[temp_classify] = recordsDS.map(
      (row:Row) => {
        val id = row.getField(0).toString
        val min_temp = row.getField(1).toString.toInt
        val max_temp = row.getField(2).toString.toInt
        val classify = row.getField(3).toString

        temp_classify(id,min_temp,max_temp,classify)
      }
    )
    jdbcDS.print()

    //5 任务执行
    senv.execute("useJDBCSource")
  }
}

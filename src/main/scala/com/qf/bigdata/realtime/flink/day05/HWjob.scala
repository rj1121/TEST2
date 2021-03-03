package com.qf.bigdata.realtime.flink.day05

import java.util.Properties
import com.qf.bigdata.realtime.flink.Myutil.MyRedisSource
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.day04.{AllCountElasticsearchSinkFunction, PersonNameRedisMapper, SpecificElasticsearchSinkFunction}
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.{PersonWide, RealTimeAll, RealTimeSpecific}
import com.qf.bigdata.realtime.flink.util.{GsonUtil, PropertyUtil}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand
import org.apache.http.HttpHost


object HWjob {


  def useKafka_Redis2ES(topic:String,standard_temp:Int,index_allCount:String,index_specificCount:String){

    //构造环境
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()

    var props:Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)

    //给定日期格式 yyyy
    var valueDeserializer = new HWKafkaDeserializationSchema("yyyyMMddHH")

    val kafkaSource = new FlinkKafkaConsumer(topic, valueDeserializer, props)
    kafkaSource.setStartFromEarliest()

    val personWideDS:DataStream[PersonWide] = senv.addSource(kafkaSource)
    val outputTag:OutputTag[PersonWide] = OutputTag[PersonWide]("specificTemp")

    //配置ES参数
    val host:java.util.List[HttpHost] = new java.util.ArrayList[HttpHost]()
    host.add(new HttpHost("192.168.44.206",9200,HttpHost.DEFAULT_SCHEME_NAME))

    //redis配置信息
    val redis_builder = new FlinkJedisPoolConfig.Builder
    redis_builder.setHost("192.168.44.206")
      .setPort(6379)
      .setDatabase(0)
      .setMinIdle(3)
      .setMaxIdle(5)
      .setMaxTotal(10)

    val redis_config = redis_builder.build()
    val redisSinkMapper:PersonNameRedisMapper = new PersonNameRedisMapper(RedisCommand.HSET)
    val redisSink:RedisSink[String] = new RedisSink(redis_config, redisSinkMapper)
    //    uv统计把name存入redis中
    personWideDS.map(_.name).addSink(redisSink)

    //分流 所有人员信息      异常人员信息
    val commonDS:DataStream[String] = personWideDS.process(

      new ProcessFunction[PersonWide,String](){

        override def processElement(value: PersonWide,
                                    ctx: ProcessFunction[PersonWide, String]#Context,
                                    out: Collector[String]): Unit = {
          val stringPersonWide:String = GsonUtil.gObject2Json(value)
          ctx.timerService()
          if(value.temp > standard_temp){

            ctx.output(outputTag,value)
          }
          out.collect(stringPersonWide)
        }

      }
    )


    //1 全部人员数据源
    val allPersonDS:DataStream[PersonWide] = commonDS.map(
      GsonUtil.gObject2Json(_,classOf[PersonWide])
    )

    var pv:Int = 0
    var uv:Int = 0
    var max_temp_allPerson:Int = 0

    val realTimeAllDS = allPersonDS.keyBy(_.age_range).map(
      (pw:PersonWide) => {
        val age_range:String = pw.age_range
        if(pw.isInstanceOf[PersonWide]){
          pv+=1
        }
        //取出Redis数据库中的Person.name总数,总数即为uv
        uv = new MyRedisSource().getUV
        max_temp_allPerson = pw.temp.max(max_temp_allPerson)

        RealTimeAll(age_range,pv,uv,max_temp_allPerson)
      }
    )

    //打印到控制台
    realTimeAllDS.print("需求1  >>>>>>>>>>   全体统计")
    //全部人员信息储存到ES

    //第一个ES配置
    val esSinkFun1 = new AllCountElasticsearchSinkFunction(index_allCount)
    val esbuilder1 = new ElasticsearchSink.Builder(host,esSinkFun1)
    esbuilder1.setBulkFlushMaxActions(1)
    val esSink1 = esbuilder1.build()

    realTimeAllDS.addSink(esSink1)

    //2 异常人员数据源
    val specificPersonDS:DataStream[PersonWide] = commonDS.getSideOutput[PersonWide](OutputTag("specificTemp"))

    var sum:Int = 0
    var max_temp_specificPerson:Int = 0
    val realTimeSpecificDS:DataStream[RealTimeSpecific] = specificPersonDS.map(
      (pw:PersonWide) => {

        val hour = pw.format_ct
        if(pw.isInstanceOf[RealTimeSpecific]){
          sum+=1
        }
        max_temp_specificPerson = max_temp_specificPerson.max(pw.temp)
        RealTimeSpecific(hour,sum,max_temp_specificPerson)
      }
    ).keyBy(_.hour)

    //打印到控制台
    realTimeSpecificDS.print("需求2  >>>>>>>>>>   异常人员统计")

    val esSinkFun2 = new SpecificElasticsearchSinkFunction(index_specificCount)
    val esbuilder2 = new ElasticsearchSink.Builder(host,esSinkFun2)
    esbuilder2.setBulkFlushMaxActions(1)
    val esSink2:ElasticsearchSink[RealTimeSpecific] = esbuilder2.build()

    //异常温度人员信息储存到ES
    realTimeSpecificDS.addSink(esSink2)

      //挂起执行
    senv.execute("useKafka_Redis2ES")

  }


  def main(args: Array[String]): Unit = {
    val topic:String = "t_tmp_201"
    val standard_temp:Int = 37

    useKafka_Redis2ES(topic,standard_temp,"allcount","specificcount")
  }

}

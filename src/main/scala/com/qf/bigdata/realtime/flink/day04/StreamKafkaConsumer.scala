package com.qf.bigdata.realtime.flink.day04

import java.util
import java.util.{Date, Properties}

import com.qf.bigdata.realtime.flink.Myutil.MyRedisSource
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC._
import com.qf.bigdata.realtime.flink.util.{CommonUtil, GsonUtil, PropertyUtil}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisMapper}
import org.apache.http.HttpHost
object StreamKafkaConsumer {

  val LOG : Logger = LoggerFactory.getLogger("")


  def useKafkaConsumer(topic:String): Unit ={

    val senv = FlinkHelper.createStreamEnv()

    val props = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)


    val schema = new SimpleStringSchema()

    //连接kafka
    val kafkaConsumer:FlinkKafkaConsumer[String] =  new FlinkKafkaConsumer[String](topic,schema,props)
    kafkaConsumer.setStartFromLatest()

    val ds:DataStream[String] = senv.addSource(kafkaConsumer)
    val personDS:DataStream[Person] = ds.map(GsonUtil.gObject2Json(_,classOf[Person]))

    personDS.print("personDS")
    senv.execute()

  }

  def useKafkaSerdeConsumer(topic:String): Unit ={
    //1 构建上下文执行环境
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()

    //2 数据源
    val props:Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)

    val schema = new KafkaDSerdePersonSchema()

    val flinkKafkaConsumer = new FlinkKafkaConsumer(topic,schema, props)
    flinkKafkaConsumer.setStartFromLatest()

    val ds:DataStream[Person] = senv.addSource(flinkKafkaConsumer)

    ds.print("useKafkaSerdeConsumer")
    senv.execute()

  }

  def useKafkaSerdeConsumerOffset(topic:String): Unit ={
    //1 构建上下文执行环境
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()

    //2 数据源为kafka
    val props:Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new KafkaSerdePersonSchema()

    val flinkKafkaConsumer = new FlinkKafkaConsumer(topic,schema, props)
    flinkKafkaConsumer.setStartFromLatest()

    //添加数据源
    val ds = senv.addSource(flinkKafkaConsumer)

    ds.print("useKafkaSerdeConsumerOffset")

    //4 任务执行
    senv.execute()

  }


  def useKafkaSerdeConsumerOffset2Redis(topic:String): Unit ={
    //1 构建环境
    val senv:StreamExecutionEnvironment = FlinkHelper.createStreamEnv()

    //2 获取数据源Source

    val props:Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new KafkaSerdePersonSchema()
    val flinkKafkaSource = new FlinkKafkaConsumer(topic,schema, props)
    flinkKafkaSource.setStartFromLatest()

    val personOffsetDS:DataStream[PersonOffset] =senv.addSource(flinkKafkaSource)

    //3 读取维护偏移量Redis

    //剥离offset信息

    val kafkaOffsetDS:DataStream[KafkaOffset] = personOffsetDS.map(
      (po:PersonOffset) => {
        KafkaOffset(po.topic,po.partition,po.offset,new Date().getTime)
      }
    )


    val builder = new FlinkJedisPoolConfig.Builder
    builder.setHost("192.168.44.206")
        .setPort(6379)
        .setDatabase(1)
        .setMinIdle(3)
        .setMaxIdle(5)
        .setMaxTotal(10)

    val config = builder.build()

    val redisSinkMapper:RJRedisMapper = new RJRedisMapper(RedisCommand.SET)
    val redisSink:RedisSink[KafkaOffset] = new RedisSink(config, redisSinkMapper)

    kafkaOffsetDS.addSink(redisSink)

    //4 挂起执行
    senv.execute("useKafkaSerdeConsumerOffset2Redis")
  }

  def useKafkaserdeConsumer2ES(topic:String,index:String): Unit ={
    val senv = FlinkHelper.createStreamEnv()

    val props:Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new KafkaSerdePersonSchema()
    val flinkKafkaSource = new FlinkKafkaConsumer(topic,schema, props)
    flinkKafkaSource.setStartFromLatest()
    val personOffsetDS:DataStream[PersonOffset] =senv.addSource(flinkKafkaSource)

    personOffsetDS.print("==============")

    val host:java.util.List[HttpHost] = new java.util.ArrayList[HttpHost]()
    host.add(new HttpHost("192.168.44.206",9200,HttpHost.DEFAULT_SCHEME_NAME))
    val esSinkFun = new RJElasticsearchSinkFunction(index)


    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered

    val esbuilder = new ElasticsearchSink.Builder(host,esSinkFun)
    esbuilder.setBulkFlushMaxActions(1)
    val esSink = esbuilder.build()
    personOffsetDS.addSink(esSink)

    senv.execute("useKafkaserdeConsumer2ES")
  }


  def useKafkaserdeConsumer2ES_mall(topic:String,index_allCount:String,index_specificCount:String,temp_validate:Int): Unit ={
    //1 构建Flink执行环境
    val senv = FlinkHelper.createStreamEnv()
    senv.setParallelism(1)

    //2 配置Kafka参数
    val props:Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    val schema = new KafkaSerdePersonSchema()
    val flinkKafkaSource = new FlinkKafkaConsumer(topic,schema, props)
    flinkKafkaSource.setStartFromLatest()

    //3  添加数据源（Kafka）
    //导入kafka数据原始信息
    val personOffsetDS:DataStream[PersonOffset] =senv.addSource(flinkKafkaSource)
    val personNameDS :DataStream[String] = personOffsetDS.map(_.name)

    //4 配置ES参数
    val host:java.util.List[HttpHost] = new java.util.ArrayList[HttpHost]()
    host.add(new HttpHost("192.168.44.206",9200,HttpHost.DEFAULT_SCHEME_NAME))



    // 需求1  >>>>>>>>>>   全体统计
    //redis  config
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
    personNameDS.addSink(redisSink)


    //加工变形为全体统计信息
    //初始化RealTimeAll信息
    var pv:Int = 0
    var uv:Int = 0
    var max_temp1 = 0
    val allCountDS:DataStream[RealTimeAll] = personOffsetDS.map(
      (po:PersonOffset) => {
        var age_range:String = ""

        //判断规则
        if(po.age < 60){
          age_range = "01"
        }else{
          age_range = "02"
        }
        if(po.isInstanceOf[PersonOffset]){
          pv+=1
        }
        //取出Redis数据库中的Person.name总数,总数即为uv
        uv = new MyRedisSource().getUV
        max_temp1 = po.temp.max(max_temp1)
        RealTimeAll(age_range,pv,uv,max_temp1)
      }
    ).keyBy(_.age_range)

    allCountDS.print("需求1  >>>>>>>>>>   全体统计")
    val esSinkFun1 = new AllCountElasticsearchSinkFunction(index_allCount)
    val esbuilder1 = new ElasticsearchSink.Builder(host,esSinkFun1)
    esbuilder1.setBulkFlushMaxActions(1)
    val esSink1 = esbuilder1.build()
    allCountDS.addSink(esSink1)


    // 需求2  >>>>>>>>>>   异常人员统计

    var sum:Int = 0
    var max_temp2 = 0
    val specificDS:DataStream[RealTimeSpecific] = personOffsetDS.filter(_.temp > temp_validate).map(
      (po:PersonOffset) =>{

        val hour = CommonUtil.formatDate4Timestamp(po.ct,"yyyyMMddHH")
        if(po.isInstanceOf[PersonOffset]){
          sum += 1
        }
        max_temp2 = po.temp.max(max_temp2)
        RealTimeSpecific(hour,sum,max_temp2)
      }
    ).keyBy(_.hour)


    specificDS.print("需求2  >>>>>>>>>>   异常人员统计")

    val esSinkFun2 = new SpecificElasticsearchSinkFunction(index_specificCount)
    val esbuilder2 = new ElasticsearchSink.Builder(host,esSinkFun2)
    esbuilder2.setBulkFlushMaxActions(1)
    val esSink2 = esbuilder2.build()

    specificDS.addSink(esSink2)

    // 挂起执行
    senv.execute("useKafkaserdeConsumer2ES_mall")
  }


  def useRedisreadData(topic:String): Unit ={

    val redis:Int = new MyRedisSource().getUV
    println(redis)

  }




  def main(args: Array[String]): Unit = {

    val topic:String = "t_tmp_201"
    val index:String = "idx_temp_201"
    useKafkaConsumer(topic)

//    useKafkaSerdeConsumer(topic)
//    useKafkaSerdeConsumerOffset(topic)

//    useKafkaSerdeConsumerOffset2Redis(topic)

//    useKafkaserdeConsumer2ES(topic,index)

//    useKafkaserdeConsumer2ES_mall(topic,"allcount","specificcount",37)

//    useKafkaserdeConsumer2ES_mall_2(topic,"allCount","specificCount",37)

  }






}

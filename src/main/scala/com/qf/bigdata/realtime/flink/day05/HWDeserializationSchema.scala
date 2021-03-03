package com.qf.bigdata.realtime.flink.day05

import java.nio.charset.StandardCharsets

import com.qf.bigdata.realtime.flink.until.StreamCC.{Person, PersonWide}
import com.qf.bigdata.realtime.flink.util.{CommonUtil, GsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class HWKafkaDeserializationSchema(format:String) extends KafkaDeserializationSchema[PersonWide]{


  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): PersonWide = {
    val key:String = new String(record.key(),StandardCharsets.UTF_8)
    val value:String = new String(record.value(),StandardCharsets.UTF_8)

//    println(s"key content is $key")

    val person:Person = GsonUtil.gObject2Json(value,classOf[Person])


    var age_range:String = ""


    if(person.age < 60){
      age_range = "01"
    }else{
      age_range = "02"
    }

    val format_ct:String = CommonUtil.formatDate4Timestamp(person.ct,format)



    PersonWide(person.name,person.age,person.temp,person.ct,age_range,format_ct)
  }

  override def isEndOfStream(nextElement: PersonWide): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[PersonWide] = {

    TypeInformation.of(classOf[PersonWide])
  }
}

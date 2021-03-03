package com.qf.bigdata.realtime.flink.day04

import java.nio.charset.StandardCharsets

import com.qf.bigdata.realtime.flink.until.StreamCC.{Person, PersonOffset}
import com.qf.bigdata.realtime.flink.util.GsonUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaSerdePersonSchema extends KafkaDeserializationSchema[PersonOffset]{
  override def isEndOfStream(nextElement: PersonOffset): Boolean = {
    return false
  }

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): PersonOffset = {
    val key:String = new String(record.key(),StandardCharsets.UTF_8)
    val value:String = new String(record.value(),StandardCharsets.UTF_8)

    val person = GsonUtil.gObject2Json(value,classOf[Person])
    val topic = record.topic()
    val partition = record.partition()
    val offset = record.offset()

    PersonOffset(person.name,person.age,person.temp,person.ct,topic,partition,offset)
  }

  override def getProducedType: TypeInformation[PersonOffset] = {
    TypeInformation.of(classOf[PersonOffset])
  }
}

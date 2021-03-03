package com.qf.bigdata.realtime.flink.day04

import java.nio.charset.StandardCharsets

import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import com.qf.bigdata.realtime.flink.util.GsonUtil
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

class KafkaDSerdePersonSchema extends DeserializationSchema[Person] with SerializationSchema[Person]{

  //反序列化
  override def deserialize(message: Array[Byte]): Person = {

    val data:String = new String(message,StandardCharsets.UTF_8)
    GsonUtil.gObject2Json(data,classOf[Person])
  }

  override def isEndOfStream(nextElement: Person): Boolean = {
    return false
  }

  //反序列化对应实体类
  override def getProducedType: TypeInformation[Person] = {
    createTypeInformation[Person]
//    TypeInformation.of(classOf[Person])
  }

  override def serialize(element: Person): Array[Byte] = {
    val json:String = GsonUtil.gObject2Json(element)
    json.getBytes(StandardCharsets.UTF_8)
  }


}

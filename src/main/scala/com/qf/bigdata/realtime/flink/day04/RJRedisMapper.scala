package com.qf.bigdata.realtime.flink.day04

import com.qf.bigdata.realtime.flink.until.StreamCC.KafkaOffset
import com.qf.bigdata.realtime.flink.util.GsonUtil
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class RJRedisMapper(command: RedisCommand) extends RedisMapper[KafkaOffset]{
  override def getCommandDescription: RedisCommandDescription = {

    new RedisCommandDescription(command,null)
  }

  override def getKeyFromData(data: KafkaOffset): String = {
    data.topic + data.partition
  }

  override def getValueFromData(data: KafkaOffset): String = {
    GsonUtil.gObject2Json(data)
  }
}

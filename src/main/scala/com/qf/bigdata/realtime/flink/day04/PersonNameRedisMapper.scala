package com.qf.bigdata.realtime.flink.day04


import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}



class PersonNameRedisMapper(command: RedisCommand) extends RedisMapper[String]{
  override def getCommandDescription: RedisCommandDescription = {

    new RedisCommandDescription(command,"PersonName")
  }

  override def getKeyFromData(data: String): String = {
    data
  }

  override def getValueFromData(data: String): String = {
    "PersonName"
  }
}
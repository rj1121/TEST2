package com.qf.bigdata.realtime.flink.day04

import com.qf.bigdata.realtime.flink.until.StreamCC.RealTimeSpecific
import com.qf.bigdata.realtime.flink.util.GsonUtil
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest


class SpecificElasticsearchSinkFunction(index:String) extends ElasticsearchSinkFunction[RealTimeSpecific]{
  override def process(element: RealTimeSpecific, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

    //索引id
    val id = element.hour + element.sum + ""

    //数据
    val data = GsonUtil.gObject2Map(element)

    //创建索引请求对象
    val indexReq : IndexRequest = new IndexRequest(index, index, id)
    indexReq.source(data)

    indexer.add(indexReq)
  }
}
package com.qf.bigdata.realtime.flink.day07

import com.qf.bigdata.realtime.flink.until.FlinkHelper
import com.qf.bigdata.realtime.flink.until.StreamCC.Person
import com.qf.bigdata.realtime.flink.util.CommonUtil
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable
import org.apache.flink.api.scala._
object StreamPartition {

  /**
    * 重分区操作
    */


    def usePartition():Unit = {
      //1 构建上下文执行环境
      val senv :StreamExecutionEnvironment = FlinkHelper.createStreamEnv()
      senv.setParallelism(3)

      //2 读取数据源
      val datas = new mutable.ListBuffer[Person]()
      for(idx <- 1 to 10){
        val name:String = CommonUtil.getRandom(1)
        val age:Int = CommonUtil.getRandomNum(2)
        val temp:Int = CommonUtil.getRandomNum(2)
        val ct:Long = CommonUtil.getRandomTimestamp
        val p = new Person(name, age, temp, ct)
        datas.+=(p)
      }
      val ds:DataStream[Person] = senv.fromCollection(datas)
//      ds.print("source")


//      ds.shuffle.print("shuffle").setParallelism(5)
//      ds.rebalance.print("rebalance")

      ds.rescale.print("rescale").setParallelism(4)

//      ds.global.print("global")
//      ds.partitionCustom(


//        new Partitioner[Int]{
//          override def partition(key: Int, numPartitions: Int): Int = {
//
//            key%numPartitions
//          }
//        },"temp"
//      ).print("Custom")

      senv.execute("")


    }


    def main(args: Array[String]): Unit = {

      usePartition()

    }
}

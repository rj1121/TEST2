package com.qf.bigdata.realtime.flink.day08

import com.qf.bigdata.realtime.flink.util.{CommonUtil, GsonUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import java.io.File

import com.qf.bigdata.realtime.flink.day07.StreamBC.Employee
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.{Accumulator, IntCounter}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import org.apache.flink.api.scala._
object StreamCacheFileAndAccumulator {


  /*
  * 累加器使用
  * */
  def useCacheFlie(filePath:String,cacheFileName:String): Unit ={
    val senv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.registerCachedFile(filePath,cacheFileName)

    val datas:mutable.ListBuffer[(String,String)] = new mutable.ListBuffer[(String,String)]()

    for(idx <- 1 to 100){
      val num:String = CommonUtil.getRandom(1)
      val name:String = CommonUtil.getRandom(1)
      datas.+=((num,name))
    }

    val ds:DataStream[(String,String)] = senv.fromCollection(datas)


    val nds = ds.process(
      new ProcessFunction[(String,String),(String,String,String)](){

        var cacheBuffer:mutable.Map[String,String] = mutable.Map[String,String]()
        var buffer:BufferedSource = _
        override def open(parameters: Configuration): Unit = {
          val cacheFile:File = this.getRuntimeContext.getDistributedCache.getFile(cacheFileName)

          buffer = Source.fromFile(cacheFile)
          val lines:Iterator[String] = buffer.getLines()

          for(line <- lines){
            val datas = line.split("\\s+|,+")
            cacheBuffer.+=((datas(0),datas(1)))

          }

        }

        override def processElement(value: (String, String),
                                    ctx: ProcessFunction[(String, String), (String, String, String)]#Context,
                                    out: Collector[(String, String, String)]): Unit = {
          val num = value._1
          val name = value._2
          val remark = cacheBuffer.getOrElse(num,"Unknown")


          out.collect(num,name,remark)
        }

        override def close(): Unit ={

          if(null != buffer){
            buffer.close()
          }
        }
      }
    )

    nds.print("useCacheFile")

    senv.execute()
  }

  def useAccumulator(): Unit ={
    //1 构建环境
    val senv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2 数据源：数据集合(内存)、本地文件、消息通道
    val employees: mutable.ListBuffer[Employee] = new mutable.ListBuffer[Employee]()

    for (idx <- 1 to 100) {
      val depNum = CommonUtil.getRandom(5)
      val name = CommonUtil.getRandom(5)
      val age = CommonUtil.getRandomNum(2)

      val employee = new Employee(depNum, name, age)
      employees.+=(employee)
    }


    val employeeDS:DataStream[Employee] = senv.fromCollection(employees)

    val accName:String = "ACC_PV"
    employeeDS.process(
      new ProcessFunction[Employee,String](){


        val accPV:IntCounter =  new IntCounter()


        override def open(parameters: Configuration): Unit ={
          this.getRuntimeContext.addAccumulator(accName,accPV)
        }



        override def processElement(value: Employee,
                                    ctx: ProcessFunction[Employee, String]#Context,
                                    out: Collector[String]): Unit = {

          accPV.add(1)
          out.collect(GsonUtil.gObject2Json(value))

        }
//        override def close(): Unit = super.close()
      }
    )

    val jobexecutionResut:JobExecutionResult = senv.execute("useAccumulator")
    val pv:Int = jobexecutionResut.getAccumulatorResult(accName)
    println(s"PV===========================$pv")
  }


  def main(args: Array[String]): Unit = {
    val filePath = "file:///E:/data/Flink/cache.txt"
    val cacheFileName = "QF_CACHE"
//    useCacheFlie(filePath,cacheFileName)

    useAccumulator()

  }
}

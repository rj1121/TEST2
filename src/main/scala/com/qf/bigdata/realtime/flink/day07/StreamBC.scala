package com.qf.bigdata.realtime.flink.day07

import com.qf.bigdata.realtime.flink.util.CommonUtil
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable

import org.apache.flink.api.scala._
object StreamBC {

  case class Employee(dep:String,name:String,age:Int)
  case class Dep(dep:String,remark:String)

  case class EmployeeDep(dep:String,name:String,age:Int,remark:String)



  def useBC(): Unit ={

    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val employees:mutable.ListBuffer[Employee] =new mutable.ListBuffer[Employee]
    val deps:mutable.ListBuffer[Dep] = new mutable.ListBuffer[Dep]()

    for(idx <- 1 to 100){
      val depNum = CommonUtil.getRandom(2)

      val name = CommonUtil.getRandom(5)
      val age = CommonUtil.getRandomNum(2)
      val remark = CommonUtil.getRandom(6)
      val employee = new Employee(depNum, name, age)
      employees.+=(employee)
      if(idx % 9 == 0){
        val dep = new Dep(depNum, remark)
        deps.+=(dep) }
    }

    //数据源

    val employeeDS:DataStream[Employee] = senv.fromCollection(employees)
    val depDS:DataStream[Dep] = senv.fromCollection(deps)


    // 广播
    val name = "BC_DEP"

    val depBCDesc:MapStateDescriptor[String,String] = new MapStateDescriptor[String,String](name,TypeInformation.of(classOf[String]),TypeInformation.of(classOf[String]))
    val depBCDS:BroadcastStream[Dep] = depDS.broadcast(depBCDesc)


    val ds:DataStream[EmployeeDep] = employeeDS.connect(depBCDS).process(

      new BroadcastProcessFunction[Employee,Dep,EmployeeDep]{
        override def processElement(value: Employee,
                                    ctx: BroadcastProcessFunction[Employee, Dep, EmployeeDep]#ReadOnlyContext,
                                    out: Collector[EmployeeDep]): Unit = {

          val dep:String = value.dep
          val name:String = value.name
          val age:Int = value.age

          val readOnlyBCState:ReadOnlyBroadcastState[String,String] = ctx.getBroadcastState(depBCDesc)
          var remark = "unknown"
          if(readOnlyBCState.contains(dep)){
            remark = readOnlyBCState.get(dep)
          }


          val ep:EmployeeDep = new EmployeeDep(dep,name,age,remark:String)
          out.collect(ep)
        }

        override def processBroadcastElement(value: Dep,
                                             ctx: BroadcastProcessFunction[Employee, Dep, EmployeeDep]#Context,
                                             out: Collector[EmployeeDep]): Unit = {

          val context:BroadcastState[String,String] = ctx.getBroadcastState(depBCDesc)
          context.put(value.dep,value.remark)

        }
      }

    )


    ds.print()

    senv.execute("useBC")

  }

  def main(args: Array[String]): Unit = {

    useBC()
  }

}

package com.qf.bigdata.realtime.flink.day07

import com.qf.bigdata.realtime.flink.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable
import org.apache.flink.api.scala._


object StreamState {


  //员工
  case class Employee(dep:String, name:String, age:Int)
  case class EmployeeAgg(pv:Long)


  def useState(): Unit ={
    //1 执行创建环境
    val senv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    senv.setParallelism(1)

    val employees:mutable.ListBuffer[Employee] = new mutable.ListBuffer[Employee]()

    for(idx <- 1 to 100){
      val depNum = CommonUtil.getRandom(1)
      val name = CommonUtil.getRandom(5)
      val age = CommonUtil.getRandomNum(2)

      val employee = new Employee(depNum, name, age)
      employees.+=(employee)
    }

    //广播
    val employeeDS:DataStream[Employee] = senv.fromCollection(employees)

    employeeDS.keyBy(_.dep)
        .process(
          new ProcessFunction[Employee,EmployeeAgg]() {


            val PV_DESC = new ValueStateDescriptor("PV_DESC_NAME",createTypeInformation[Long])
            var PV_STATE:ValueState[Long] = _

            override def open(parameters: Configuration): Unit = {

              PV_STATE = this.getRuntimeContext.getState[Long](PV_DESC)
            }



            override def processElement(value: Employee,
                                        ctx: ProcessFunction[Employee, EmployeeAgg]#Context,
                                        out: Collector[EmployeeAgg]): Unit = {

              val name = value.name
              val initAccess = 0L
              var last:Long = PV_STATE.value
              if(last == null){
                PV_STATE.update(initAccess)
              }

              if(StringUtils.isNoneEmpty(name)){
                last += 1
              }

              out.collect(new EmployeeAgg(last))
              PV_STATE.update(last)
            }

            override def close(): Unit = {
//              PV_STATE.clear()
            }
          }

        ).print("")

    //5 挂起执行
    senv.execute("useState")
  }




  def main(args: Array[String]): Unit = {


    useState()
  }

}

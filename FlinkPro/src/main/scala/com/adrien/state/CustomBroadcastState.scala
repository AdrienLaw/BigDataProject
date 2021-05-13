package com.adrien.state


import com.adrien.processfunction.{Class, Student, TestBroadcastProcessFunction}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

/**
 * 参数
 * 未广播数据类型
 * 广播数据类型
 * 输出数据类型
 */
class CustomBroadcastState  extends BroadcastProcessFunction[Student,Class,String]{

  private val mapStateDescriptor = new MapStateDescriptor[String,String]("maps", Types.of[String], Types.of[String])

  override def processElement(value: Student, ctx: BroadcastProcessFunction[Student, Class, String]#ReadOnlyContext, out: Collector[String]): Unit = {

    val classInfo = ctx.getBroadcastState(TestBroadcastProcessFunction.descriptor)

    val className: String = classInfo.get(value.classId)

    out.collect(s"stuId:${value.id}  stuName:${value.name} stuClassName:${className}")
  }

  override def processBroadcastElement(value: Class, ctx: BroadcastProcessFunction[Student, Class, String]#Context, out: Collector[String]): Unit = {

    val classInfo: BroadcastState[String, String] = ctx.getBroadcastState(mapStateDescriptor)
    println("更新状态")
    classInfo.put(value.id,value.name)
  }
}

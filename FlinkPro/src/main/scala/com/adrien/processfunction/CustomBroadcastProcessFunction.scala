package com.adrien.processfunction

import com.adrien.sink.kafka.Student
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class CustomBroadcastProcessFunction
  extends BroadcastProcessFunction[Student,Class,String]{
  override def processElement(value: Student, ctx: BroadcastProcessFunction[Student, Class, String]#ReadOnlyContext, out: Collector[String]): Unit = {

    val classInfo = ctx.getBroadcastState(TestBroadcastProcessFunction.descriptor)

    val className: String = classInfo.get(value.classId)

    out.collect(s"stuId:${value.id}  stuName:${value.name} stuClassName:${className}")
  }

  override def processBroadcastElement(value: Class, ctx: BroadcastProcessFunction[Student, Class, String]#Context, out: Collector[String]): Unit = {

    val classInfo = ctx.getBroadcastState(TestBroadcastProcessFunction.descriptor)
    println("更新状态")
    classInfo.put(value.id,value.name)
  }
}
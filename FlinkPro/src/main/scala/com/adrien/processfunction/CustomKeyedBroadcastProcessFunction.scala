package com.adrien.processfunction

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

class CustomKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,Student,Class,String]{
  override def processElement(value: Student, ctx: KeyedBroadcastProcessFunction[String, Student, Class, String]#ReadOnlyContext, out: Collector[String]): Unit = {
    println(s"processElement.key = ${ctx.getCurrentKey}")
    val classInfo = ctx.getBroadcastState(TestKeyedBroadcastProcessFunction.descriptor)
    val className: String = classInfo.get(value.classId)
    out.collect(s"stuId:${value.id}  stuName:${value.name} stuClassName:${className}")
  }

  override def processBroadcastElement(value: Class, ctx: KeyedBroadcastProcessFunction[String, Student, Class, String]#Context, out: Collector[String]): Unit = {
    val classInfo = ctx.getBroadcastState(TestKeyedBroadcastProcessFunction.descriptor)
    println("更新状态")
    classInfo.put(value.id,value.name)
  }
}

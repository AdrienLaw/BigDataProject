package com.adrien.state


import com.adrien.processfunction.{Class, Student}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestBroadcastState extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[String] = env.socketTextStream("localhost",8888)

  private val StudentStream: DataStream[Student] = stream1
    .map(data => {
      val arr = data.split(",")
      Student(arr(0), arr(1), arr(2))
    })

  val descriptor = new MapStateDescriptor[String,  String]("classInfo", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  val ClassStream: DataStream[Class] = stream2.map(data => {
    val arr = data.split(",")
    Class(arr(0), arr(1))
  })
  val ClassBradoStream: BroadcastStream[Class] = ClassStream.broadcast(descriptor)

  StudentStream
    .connect(ClassBradoStream)
    .process(new CustomBroadcastState)
    .print("CustomBroadcastState")

  env.execute()
}

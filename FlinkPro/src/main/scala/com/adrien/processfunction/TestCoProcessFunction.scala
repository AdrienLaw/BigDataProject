package com.adrien.processfunction

import com.adrien.window.{Obj1, Record}
import org.apache.flink.streaming.api.scala._

object TestCoProcessFunction extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[String] = env.socketTextStream("localhost",8888)
  private val stream1Obj: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  val stream2Rec: DataStream[Record] = stream2.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })
  print("================")
  stream1Obj
    .connect(stream2Rec)
    .process(new CustomCoProcessFunction)
    .print()
  env.execute()
}

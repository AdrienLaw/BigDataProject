package com.adrien.processfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala._

object TestKeyedProcessFunction extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
    .keyBy(_.id)
    .process(new CustomKeyedProcessFunction)
    .print("TestKeyedProcessFunction")
  env.execute()
}

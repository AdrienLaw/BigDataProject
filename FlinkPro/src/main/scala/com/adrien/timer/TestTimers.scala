package com.adrien.timer

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala._

object TestTimers extends App {
  // 创建执行环境
  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  private val value: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  private val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)
  value1.process(new CustomKeyedProcessFunction)
    .print("TestKeyedProcessFunction")
  env.execute()
}

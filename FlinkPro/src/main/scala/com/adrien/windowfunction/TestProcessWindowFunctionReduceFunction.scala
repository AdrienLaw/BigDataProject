package com.adrien.windowfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestProcessWindowFunctionReduceFunction extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })
  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(_.id)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .reduce(new MinTimeReduceFunction2,new CustomProcessFunction2)
    .print("TestProcessWindowFunctionReduceFunction")
  environment.execute()
}

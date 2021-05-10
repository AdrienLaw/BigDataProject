package com.adrien.processfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestProcessWindowFunction  extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })
  // 设置一个窗口时间是 5 秒的窗口
  stream2
    .keyBy(_.id)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .process(new CuntomProcessFunction)
    .print("TestWindowFunction")
  environment.execute()
}

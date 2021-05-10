package com.adrien.windowfunction

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestWindowFunction extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
  // 设置一个窗口时间是 5 秒的窗口
  stream1
    .flatMap(_.split(","))
    .map((_,1))
    .keyBy(t =>t._1)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .apply(new CustomWindowFunction)
    .print("TestWindowFunction")
  environment.execute()
}

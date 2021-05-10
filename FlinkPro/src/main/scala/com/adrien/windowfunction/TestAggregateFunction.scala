package com.adrien.windowfunction

import com.adrien.window.Record
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object TestAggregateFunction extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
  stream1.print("print")
  val stream2: DataStream[Record] = stream1.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })
  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .aggregate(new AgeAverageAggregateFunction)
    .print("TestReduceFunction")
  environment.execute()
}

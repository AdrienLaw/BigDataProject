package com.adrien.windowfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object TestProcessWindowAllFunction  extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })
  // 设置一个窗口时间是 5 秒的窗口
  private val allValueStream: AllWindowedStream[Obj1, TimeWindow] = stream2
    .keyBy(_.id)
    .timeWindowAll(Time.seconds(5))

  allValueStream
    .process(new CustomProcessAllWindowFunction)
    .print("TestProcessWindowAllFunction")

  environment.execute()
}

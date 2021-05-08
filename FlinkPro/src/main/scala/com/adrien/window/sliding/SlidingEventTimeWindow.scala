package com.adrien.window.sliding


import com.adrien.window.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingEventTimeWindow extends App {
  val enev: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //当使用 SlidingEventTimeWindows 窗口时一定要先设置 EventTime 否则会抛出异常
  enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream01: DataStream[String] = enev.socketTextStream("",9001)
  stream01.print("stream01")
  val stream02: DataStream[Obj1] = stream01.map(data => {
    val arr = data.split(",")
    //println(arr)
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Obj1] {
    override def extractAscendingTimestamp(element: Obj1) = {
      // 提取当前的 EventTime，会设置当前的 EventTime 为 WaterMark
      element.time * 1000
    }
  })
  stream02.keyBy(0)
    .window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(3)))
    .reduce(new MinDataReduceFunction)
    .print("SlidingEventTimeWindow")
  enev.execute()

}

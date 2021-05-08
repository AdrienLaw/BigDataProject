package com.adrien.window.tumbling



import com.adrien.window.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object TumblingEventTimeWindow extends App {
  val enev:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream01: DataStream[String] = enev.socketTextStream("hadoop101",9001)
  stream01.print("stream1")
  val stream02: DataStream[Obj1] = stream01.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  })
    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Obj1] {
    override def extractAscendingTimestamp(element: Obj1) = {
      // 提取当前的 EventTime，会设置当前的 EventTime 为 WaterMark
      element.time * 1000
    }
  })
  stream02.keyBy(0)
    .window(TumblingTimeWindows.of(Time.seconds(6)))
    .reduce(new MinDataReduceFunction)
    .print("TestTumblingTimeWindow")
  enev.execute()
}

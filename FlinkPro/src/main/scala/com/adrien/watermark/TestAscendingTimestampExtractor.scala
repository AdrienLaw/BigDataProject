package com.adrien.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor

object TestAscendingTimestampExtractor extends App {
  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream01: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[Obj2] = stream01.map(data => {
    val arr = data.split(",")
    Obj2(arr(0), arr(1).toLong)
  }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Obj2] {
    override def extractAscendingTimestamp(element: Obj2): Long = {
      // 提取当前的 EventTime，会设置当前的 EventTime 为 WaterMark
      element.time + 2000
    }
  })
}

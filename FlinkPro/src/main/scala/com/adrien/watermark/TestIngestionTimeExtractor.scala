package com.adrien.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor

object TestIngestionTimeExtractor extends App {
  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用 IngestionTime 时间语义
  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[Obj4] = stream1.map(data => {
    val arr = data.split(",")
    Obj4(arr(0), arr(1).toLong)
  }).assignTimestampsAndWatermarks(new IngestionTimeExtractor[Obj4])
  stream2.print()
  env.execute()
}

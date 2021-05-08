package com.adrien.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object TestBoundedOutOfOrdernessTimestampExtractor extends App {
  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj3] = stream1.map(data => {
    val arr = data.split(",")
    Obj3(arr(0), arr(1).toLong)
  }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Obj3](Time.seconds(3)
    ) {
      override def extractTimestamp(element: Obj3) = element.time * 1000
    })
  env.execute()
}

package com.adrien.watermark

import org.apache.flink.streaming.api.TimeCharacteristic

object TestCustomAssignerWithPeriodicWatermarks {
  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj5] = stream1.map(data => {
    val arr = data.split(",")
    Obj5(arr(0), arr(1).toLong)
  }).assignTimestampsAndWatermarks(new CustomPeriodicAssiner)
  env.execute()
}

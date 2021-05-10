package com.adrien.processfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TestProcessJoinFunction extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[String] = env.socketTextStream("localhost",8888)

  private val stream1Obj: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Obj1](Time.seconds(3)) {
    override def extractTimestamp(element: Obj1) = element.time * 1000
  })

  val stream1Obj2: DataStream[Obj1] = stream2.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Obj1](Time.seconds(3)) {
    override def extractTimestamp(element: Obj1) = element.time * 1000
  })
  private val value: KeyedStream[Obj1, String]#IntervalJoined[Obj1, Obj1, String] = stream1Obj
    .keyBy(_.name)
    // 指定时间区间 join 数据
    .intervalJoin(stream1Obj2.keyBy(_.name))
    // 设置时间范围 从 EventTime前10分钟，到EventTime 时间
    .between(Time.minutes(-10), Time.seconds(0))
  // 不包含下界
  //.lowerBoundExclusive()
  // 不包含上界
  //.upperBoundExclusive()
  // 自定义ProcessJoinFunction 处理Join到的元素
  value.process(new CustomProcessJoinFunction).print("TestProcessJoinFunction")
  env.execute()
}

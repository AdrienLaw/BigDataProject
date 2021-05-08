package com.adrien.evictor

import com.adrien.window.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor
import org.apache.flink.streaming.api.windowing.time.Time

object TestDeltaEvictor extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  private val deltaFunction: DeltaFunction[Obj1] = new DeltaFunction[Obj1] {
    override def getDelta(oldDataPoint: Obj1, newDataPoint: Obj1) = {
      newDataPoint.time - oldDataPoint.time
    }
  }
  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    // 使用 DeltaEvictor移除器,设置在开窗之前移除数据
    .evictor(DeltaEvictor.of(10,deltaFunction,false))
    .reduce(new MinDataReduceFunction)
    .print()
  environment.execute()
}

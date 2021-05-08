package com.adrien.window.session

import com.adrien.window.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows


object SessionProcessingTimeSessionWindow {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream01: DataStream[String] = env.socketTextStream("localhost",9999)
    stream01.print()
    val stream02: DataStream[Obj1] = stream01.map(data => {
      val splits = data.split(",")
      Obj1(splits(0), splits(1), splits(2).toLong)
    })
    stream02.keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .reduce(new MinDataReduceFunction)
      .print("ProcessingTimeSessionWindows")
    env.execute()
  }
}

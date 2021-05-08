package com.adrien.window.wordcount

import com.adrien.sources.SensorReading
import com.adrien.window.Record
import org.apache.flink.streaming.api.scala._

object TumblingCountWindow {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream01: DataStream[String] = env.socketTextStream("localhost",9999)
    val stream02: DataStream[Record] = stream01.map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })
    // 取出 2 条记录之内,每个 classId 年纪最小的用户
    stream02.map(record => {
      (record.id,record.name,record.age)
    }).keyBy(_._1)
      //  窗口数量 默认使用的是 processing time
      .countWindow(2)
      .reduce((r1,r2)=>{(r1._1,r2._1,r1._3.min(r2._3))})
      .print("")
    env.execute()
  }
}

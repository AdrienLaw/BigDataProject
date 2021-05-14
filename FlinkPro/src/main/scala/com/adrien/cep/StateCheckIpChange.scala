package com.adrien.cep

import org.apache.flink.streaming.api.scala._

object StateCheckIpChange extends App {
  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setParallelism(1)
  // 1. 添加 socket 数据源
  val sourceStream: DataStream[String] = environment.socketTextStream("localhost",9999)
  import org.apache.flink.api.scala._
  // 2. 数据处理
  sourceStream.map(
    x =>{
      val strings: Array[String] = x.split(",")
      (strings(1),UserLogin(strings(0),strings(1),strings(2),strings(3)))
    }
  ).keyBy(x => x._1)
    .process(new CheckIpChangeProcessFunction)
    .print()
  environment.execute("checkIpChange")
}

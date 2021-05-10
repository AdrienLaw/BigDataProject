package com.adrien.partitioner

import org.apache.flink.streaming.api.scala._

object TestForwardPartitioner  extends App{
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List(1,2,3,4,5))
  // 直接打印数据
  stream.map(v=>{v+1}).setParallelism(2).print().setParallelism(2)
  /**
   * 2> 2
   * 1> 3
   * 2> 4
   * 1> 5
   * 2> 6
   */
  env.execute()
}
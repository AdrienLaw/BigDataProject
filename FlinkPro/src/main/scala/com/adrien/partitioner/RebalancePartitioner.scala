package com.adrien.partitioner
import org.apache.flink.streaming.api.scala._

object RebalancePartitioner extends App{
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List(1,2,3,4,5,6))
  // 直接打印数据
  stream.rebalance.print().setParallelism(2)
  /**
   * 2> 2
   * 2> 4
   * 1> 1
   * 2> 6
   * 1> 3
   * 1> 5
   */
  env.execute()
}

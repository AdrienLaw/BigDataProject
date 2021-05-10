package com.adrien.partitioner

import org.apache.flink.streaming.api.scala._

object TestKeyGroupStreamPartitioner extends App{
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List(1,2,3,4,5,6))
  val stream2 = stream.map(v=>{(v%2,v)})
  /**
   * key:9> (1,1)
   * key:9> (1,3)
   * key:9> (1,5)
   * key:9> (0,2)
   * key:9> (0,4)
   * key:9> (0,6)
   */
  stream2.setParallelism(2).keyBy(_._1).print("key")
  env.execute()
}

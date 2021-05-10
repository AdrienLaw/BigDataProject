package com.adrien.partitioner

import org.apache.flink.streaming.api.scala._


object TestCustomPartitioner extends App{
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List("1","2","3","4","5"))
  val partitioner = CustomPartitioner
  val stream2 = stream.map(value=>{((value.toInt%2).toString,value)})
  stream2.partitionCustom(partitioner,0).print().setParallelism(2)
  /**
   * 2> (1,1)
   * 1> (0,2)
   * 2> (1,5)
   * 1> (0,4)
   * 2> (1,3)
   */
  env.execute()
}

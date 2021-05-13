package com.adrien.state


import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestListStates extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  import org.apache.flink.runtime.state.filesystem.FsStateBackend
  import org.apache.flink.streaming.api.CheckpointingMode
  import org.apache.flink.streaming.api.environment.CheckpointConfig

  env.enableCheckpointing(5000)
  // 使用文件存储的状态后端
  val stateBackend = new FsStateBackend("file:///opt/flink-1.10.2/checkpoint",true)
  env.setStateBackend(stateBackend)
  // 设置检查点模式（精确一次 或 至少一次）
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
  // 设置两次检查点尝试之间的最小暂停时间
  env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
  // 设置检查点超时时间
  env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)
  // 设置可能同时进行的最大检查点尝试次数
  env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  // 使检查点可以在外部保留
  env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  private val stream: DataStream[String] = env.addSource(new WordDataSourceWithState)
  stream.print("TestListState")
  env.execute()
}

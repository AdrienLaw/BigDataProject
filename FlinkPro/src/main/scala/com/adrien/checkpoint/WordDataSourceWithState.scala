package com.adrien.checkpoint

import java.lang
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class WordDataSourceWithState  extends RichSourceFunction[String] with CheckpointedFunction{
  var isCancel:Boolean = _
  val words = ArrayBuffer("hadoop", "spark", "linux", "flink", "flume", "oozie", "kylin")
  var totalCount:BigInt = _
  var random:Random = _

  import org.apache.flink.api.common.state.ListState
  var listState: ListState[BigInt] = null

  override def open(parameters: Configuration): Unit = {
    isCancel = false
    totalCount = 0
    random = new Random
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while(!isCancel){ // 如果 source 启动，
      if (totalCount.intValue() % 10 == 0){
        // 发送数据
        ctx.collect("primitive");
      }else{
        ctx.collect(words(random.nextInt(words.length)))
      }
      totalCount = totalCount+1
      Thread.sleep(random.nextInt(3000))
    }
  }
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // 快照状态
    listState.clear
    listState.add(totalCount)
    println(s"保存状态，totalCount=${totalCount}")
  }

  /**
   * initializeState 方法接收一个 FunctionInitializationContext 参数，
   * 会用来初始化 non-keyed state 的 "容器"。
   * 这些容器是一个 ListState 用于在 checkpoint 时保存 non-keyed state 对象。
   * @param context
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    import org.apache.flink.api.common.state.ListStateDescriptor
    import org.apache.flink.api.common.typeinfo.TypeInformation
    println("初始化状态")
    // 1. 构建StateDesccriptor
    val totalCountListStateDescriptor = new ListStateDescriptor[BigInt]("total_count", TypeInformation.of(classOf[BigInt]))
    // 2. 构建Operator State 使用的是 Even-split redistribution 数据分布模式
    listState = context.getOperatorStateStore().getListState(totalCountListStateDescriptor)

    // 2. 构建Operator State 使用的是 Union redistribution 数据分布模式
    // listState = context.getOperatorStateStore().getListState(totalCountListStateDescriptor)

    val iterTotalCnt2: lang.Iterable[BigInt] = listState.get
    val has = iterTotalCnt2.iterator().hasNext
    println(s"has=$has")

    println(s"context.isRestored()=${context.isRestored()}")
    // 恢复 totalCount
    if (context.isRestored()){
      println("恢复数据")
      val iterTotalCnt: lang.Iterable[BigInt] = listState.get

      import java.util
      val iterator: util.Iterator[BigInt] = iterTotalCnt.iterator
      if (iterator.hasNext) {

        totalCount = iterator.next
        println(s"恢复数据，totalCount=${totalCount}")
      }
    }
  }

  override def cancel(): Unit = {
    isCancel = true
  }
}

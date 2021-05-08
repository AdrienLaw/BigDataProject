package com.adrien.trigger

import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class CustomTrigger[Window] extends Trigger[Object,TimeWindow] {
  //触发计算的最大数量
  private var maxCount: Long = _
  //定时触发间隔时长 (ms)
  private var interval: Long = 60 * 1000

  /**
   * @param maxCount 触发窗口计算的数据量
   * @param interval 触发窗口计算的时间间隔 单位秒
   */
  def this(maxCount: Long,interval:Long) {
    this()
    this.maxCount = maxCount
    this.interval = interval * 1000
  }
  // 记录当前数量的状态
  private val countStateDescriptor:ReducingStateDescriptor[Long]  = new ReducingStateDescriptor[Long]("counter", new Sum, classOf[Long])
  // 记录 processTime 定时触发时间的状态
  private val processTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("processTimer", new Update, classOf[Long])
  // 记录 eventTime 定时触发时间的状态
  private val eventTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("eventTimer", new Update, classOf[Long])

  override def onElement(element: Object, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val countState: ReducingState[Long] = ctx.getPartitionedState(countStateDescriptor)
    countState.add(1L) //计数状态加1

    println(s"当前数据量为：${countState.get()}")

    if (countState.get() >= this.maxCount) {
      //达到指定指定数量
      clear(window,ctx)
      println(s"数据量达到 $maxCount ， 触发计算")
      //触发计算
      TriggerResult.FIRE_AND_PURGE
    } else if (ctx.getPartitionedState(processTimerStateDescriptor).get() == 0L) {

      val nextFire = ctx.getCurrentProcessingTime + interval
      println(s"未达到指定数量，设置下次触发计算的时间为：${nextFire}")
      //未达到指定数量，且没有指定定时器，需要指定定时器
      //当前定时器状态值加上间隔值
      ctx.getPartitionedState(processTimerStateDescriptor).add(nextFire)
      //注册定执行时间定时器
      ctx.registerProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
      TriggerResult.CONTINUE
    } else {
      TriggerResult.CONTINUE
    }
  }

  // ProcessingTime 定时器触发
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if(0 == ctx.getPartitionedState(processTimerStateDescriptor).get()){
      println("还没有指定下次触发窗口计算的时间")
    }else{
      println(s"距离触发下次窗口计算还有（ ${ctx.getPartitionedState(processTimerStateDescriptor).get().-(time)/1000} ）秒")
    }

    // 如果计数器数量大于0 并且 当前时间大于等于触发时间
    if (ctx.getPartitionedState(countStateDescriptor).get() > 0 && (ctx.getPartitionedState(processTimerStateDescriptor).get() <= time)) {
      println(s"数据量未达到 $maxCount ,由执行时间触发器 ${ctx.getPartitionedState(processTimerStateDescriptor).get() } 触发计算")
      clear(window,ctx)
      TriggerResult.FIRE_AND_PURGE
    } else {
      TriggerResult.CONTINUE
    }
  }


  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  /**
   * 窗口结束时清空状态
   * @param window
   * @param ctx
   */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // 清理数量状态
    ctx.getPartitionedState(countStateDescriptor).clear()
    // 清理 processTimer 状态
    ctx.getPartitionedState(processTimerStateDescriptor).clear()
  }
}

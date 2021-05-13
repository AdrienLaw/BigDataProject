package com.adrien.timer

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * KeyedProcessFunction
 * String, 输入的 key 的数据类型
 * Obj1, 输入的数据类型
 * String 输出的数据类型
 */
class CustomKeyedProcessFunction extends KeyedProcessFunction[String, Obj1, String]{
  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]): Unit = {
    println(s"当前 key:${ctx.getCurrentKey}")
    println(s"当前 ProcessingTime:${ctx.timerService().currentProcessingTime()}")

    if (value.id == "1") {
      val timestamp = System.currentTimeMillis() + 10000
      println(s"设置定时器，触发时间为：$timestamp")
      ctx.timerService().registerProcessingTimeTimer(timestamp);
    }
    out.collect(value.name)
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Obj1, String]#OnTimerContext, out: Collector[String]): Unit = {
    println(s"定时器触发，时间为：$timestamp")
    out.collect(s"$timestamp")
  }
}

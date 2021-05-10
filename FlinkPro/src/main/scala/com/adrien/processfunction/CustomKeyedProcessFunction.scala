package com.adrien.processfunction

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
  override def processElement(value: Obj1,
                              ctx: KeyedProcessFunction[String, Obj1, String]#Context,
                              out: Collector[String]): Unit = {
    println(s"当前 key:${ctx.getCurrentKey}")
    println(s"当前 ProcessingTime:${ctx.timerService().currentProcessingTime()}")
    out.collect(value.name)
  }
}

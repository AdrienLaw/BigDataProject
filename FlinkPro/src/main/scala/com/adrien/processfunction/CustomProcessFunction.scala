package com.adrien.processfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class CustomProcessFunction extends ProcessFunction[Obj1,String]{
  /**
   * 处理流中的每个数据，返回 ID 大于 10 的数据与处理数据的 processTime
   * @param value
   * @param ctx
   * @param out
   */
  override def processElement(value: Obj1, ctx: ProcessFunction[Obj1, String]#Context, out: Collector[String]): Unit = {
    if(value.id > "10"){
      out.collect(s"${value.name},${ctx.timerService().currentProcessingTime()}")
    }
  }
}

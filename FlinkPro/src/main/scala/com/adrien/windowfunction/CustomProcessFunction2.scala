package com.adrien.windowfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class CustomProcessFunction2 extends ProcessWindowFunction[Obj1, (Long, Obj1), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Obj1], out: Collector[(Long, Obj1)]): Unit = {
    val min = elements.iterator.next
    out.collect((context.window.getStart, min))
  }
}

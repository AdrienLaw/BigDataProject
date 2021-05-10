package com.adrien.windowfunction

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * (String,Int) –输入值的类型
 * String –输出值的类型
 * String key 类型 –密钥的类型
 * TimeWindow window 类型 –可以应用此窗口功能的 Window 类型
 */
class CustomWindowFunction extends WindowFunction[(String,Int),String,String,TimeWindow]{
  val sdf = new SimpleDateFormat("HH:mm:ss")
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[(String, Int)],
                     out: Collector[String]): Unit = {
    println(
      s"""
         |window key：${key},
         |开始时间：${sdf.format(window.getStart)},
         |结束时间：${sdf.format(window.getEnd)},
         |maxTime：${sdf.format(window.maxTimestamp())}
         |""".stripMargin)

    out.collect(s"${key},${input.map(_._2).sum}")
  }
}

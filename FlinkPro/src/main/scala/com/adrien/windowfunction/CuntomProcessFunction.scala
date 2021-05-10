package com.adrien.windowfunction

import java.text.SimpleDateFormat

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 * IN –输入值的类型。
 * OUT –输出值的类型。
 * KEY –密钥的类型。
 * W –窗口的类型
 */
class CuntomProcessFunction extends ProcessWindowFunction[Obj1, String, String, TimeWindow]  {
  override def process(key: String, context: Context,
                       elements: Iterable[Obj1], out: Collector[String]): Unit = {
    var count = 0
    val sdf = new SimpleDateFormat("HH:mm:ss")

    println(
      s"""
         |window key：${key},
         |开始时间：${sdf.format(context.window.getStart)},
         |结束时间：${sdf.format(context.window.getEnd)},
         |maxTime：${sdf.format(context.window.maxTimestamp())}
         |""".stripMargin)

    // 遍历，获得窗口所有数据
    for (obj <- elements) {
      println(obj.toString)
      count += 1
    }
    out.collect(s"Window ${context.window} , count : ${count}")
  }
}

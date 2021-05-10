package com.adrien.windowfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

/**
 * 定义一个ProcessAllWindowFunction
 * 把窗口内所有用户名 拼接成字符串 用 "," 分隔
 * 输入类型 obj1
 * 输出类型  元组(Long,String)
 * 窗口类型 TimeWindow
 */
class CustomProcessAllWindowFunction  extends ProcessAllWindowFunction[Obj1, (Long,String), TimeWindow]{
  override def process(context: Context, elements: Iterable[Obj1], out: Collector[(Long, String)]): Unit = {

    println(s"start:${context.window.getStart}")
    println(s"end:${context.window.getEnd}")
    println(s"maxTimestamp:${context.window.maxTimestamp()}")
    val key =  context.window.getStart
    val value = new ArrayBuffer[String]()

    for (obj1<- elements){
      value.append(obj1.name)
    }
    // 把窗口内所有用户名 拼接成字符串 用 "," 分隔
    out.collect((key,value.mkString(",")))
  }
}

package com.adrien.processfunction

import com.adrien.window.{Obj1, Record}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
/**
 * 第一个流输入类型为 Obj1
 * 第二个流输入类型为 Record
 * 返回类型为 String
 */
class CustomCoProcessFunction  extends CoProcessFunction[Obj1,Record,String] {
  override def processElement1(value: Obj1, ctx: CoProcessFunction[Obj1, Record, String]#Context, out: Collector[String]): Unit = {
    out.collect(s"processElement1:${value.name},${value.getClass}")
  }

  override def processElement2(value: Record, ctx: CoProcessFunction[Obj1, Record, String]#Context, out: Collector[String]): Unit = {
    out.collect(s"processElement2:${value.name},${value.getClass}")
  }
}

package com.adrien.cep


import java.util
import org.apache.flink.cep.{PatternFlatTimeoutFunction}
import org.apache.flink.util.Collector

// 自定义超时事件序列处理函数
class FlatSelectTimeoutFunction extends PatternFlatTimeoutFunction[Record,String]{
  override def timeout(pattern: util.Map[String, util.List[Record]], timeoutTimestamp: Long, out: Collector[String]): Unit = {
    out.collect(
      s"""
         |flatSelectFunction:
         |  ${pattern.get("start").toString}
         |""".stripMargin)
  }
}

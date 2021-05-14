package com.adrien.cep

import java.util

import org.apache.flink.cep.PatternFlatSelectFunction
import org.apache.flink.util.Collector

/**
 * 定义一个 PatternFlatSelectFunction
 */
class FlatSelectFunction  extends PatternFlatSelectFunction[Record,String]{
  override def flatSelect(pattern: util.Map[String, util.List[Record]], out: Collector[String]): Unit = {
    out.collect(
      s"""
         |flatSelectFunction:
         |  ${pattern.get("start").toString},
         |  ${pattern.get("next").toString}
         |""".stripMargin)
  }
}

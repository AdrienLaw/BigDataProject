package com.adrien.cep


import java.util
import org.apache.flink.cep.PatternSelectFunction

class selectFunction  extends PatternSelectFunction[Record,String] {
  override def select(pattern: util.Map[String, util.List[Record]]): String = {
    // 从 map 中根据名称获取对应的事件
    val start: Record = pattern.get("start").iterator().next()
    start.toString
  }
}

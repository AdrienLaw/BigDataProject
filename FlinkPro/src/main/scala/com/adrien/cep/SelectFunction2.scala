package com.adrien.cep

import java.util
import org.apache.flink.cep.PatternSelectFunction

// 自定义正常事件序列处理函数
class SelectFunction2 extends PatternSelectFunction[Record,String] {
  override def select(pattern: util.Map[String, util.List[Record]]): String = {
    val start: Record = pattern.get("start").iterator().next()
    s"${start.toString},success"
  }
}

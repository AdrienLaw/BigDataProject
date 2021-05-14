package com.adrien.cep

import java.util


import org.apache.flink.cep.PatternSelectFunction

/**
 * 定义一个Pattern Select Function 对匹配到的数据进行处理
 */
class PatternMacthData extends PatternSelectFunction[Record,String]{
  override def select(pattern: util.Map[String, util.List[Record]]): String = {
    // 从 map 中根据名称获取对应的事件
    val start: Record = pattern.get("start").iterator().next()
    val next: Record = pattern.get("next").iterator().next()
    s"${start.name} 与 ${next.name} 都是 ${start.age} 岁"
  }
}

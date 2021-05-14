package com.adrien.cep

import java.util
import org.apache.flink.cep.PatternTimeoutFunction

class OutTimeSelectFunction2 extends PatternTimeoutFunction[Record,String]{
  override def timeout(pattern: util.Map[String, util.List[Record]], l: Long): String = {
    val start: Record = pattern.get("start").iterator().next()
    s"${start.toString},outTime"
  }
}

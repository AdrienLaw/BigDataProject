package com.adrien.cep

import java.util
import org.apache.flink.cep.{PatternTimeoutFunction}

class OrderTimeoutPatternFunction extends PatternTimeoutFunction[OrderDetail,OrderDetail]{
  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], l: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    detail
  }
}

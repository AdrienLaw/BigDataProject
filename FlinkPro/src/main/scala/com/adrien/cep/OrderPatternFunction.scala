package com.adrien.cep

import java.util
import org.apache.flink.cep.{PatternSelectFunction}

/**
 * 获取成功支付的订单
 */
class OrderPatternFunction  extends PatternSelectFunction[OrderDetail,OrderDetail] {
  override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail: OrderDetail = pattern.get("second").iterator().next()
    detail
  }
}

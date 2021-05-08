package com.adrien.trigger

import org.apache.flink.api.common.functions.ReduceFunction

/**
 * 更新状态为最新的时间戳
 */
class Update extends ReduceFunction[Long] {
  override def reduce(value1: Long, value2: Long): Long = value2
}

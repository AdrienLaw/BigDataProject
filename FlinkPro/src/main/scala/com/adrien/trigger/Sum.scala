package com.adrien.trigger

import org.apache.flink.api.common.functions.ReduceFunction

/**
 * 计数方法 记录当前数据量
 */
class Sum extends ReduceFunction[Long] {
  override def reduce(value1: Long, value2: Long): Long = value1 + value2
}
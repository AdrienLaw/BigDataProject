package com.adrien.partitioner

import org.apache.flink.api.common.functions.Partitioner

object CustomPartitioner  extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    // 根据 key值的 奇偶 返回到不同的分区
    key.toInt % 2
  }
}

package com.adrien.windowfunction

import com.adrien.window.Record
import org.apache.flink.api.common.accumulators.AverageAccumulator
import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable.ArrayBuffer

/**
 * 使用 AggregateFunction 计算最近十秒内流入数据的用户的平均年龄
 */
class AgeAverageAggregateFunction extends AggregateFunction[Record, AverageAccumulator, (ArrayBuffer[Record], Double)] {

  override def getResult(accumulator: AverageAccumulator): (ArrayBuffer[Record], Double)= {//
    val avg = accumulator.sum./(accumulator.count.toDouble)

    (accumulator.records,avg)

  }
  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
    a.count += b.count
    a.sum += b.sum
    a.records.appendAll(b.records)
    a
  }
  override def createAccumulator(): AverageAccumulator = {
    new AverageAccumulator()
  }
  override def add(value: Record, accumulator: AverageAccumulator): AverageAccumulator = {
    accumulator.records.append(value)
    accumulator.sum += value.age
    accumulator.count+=1
    accumulator
  }
}
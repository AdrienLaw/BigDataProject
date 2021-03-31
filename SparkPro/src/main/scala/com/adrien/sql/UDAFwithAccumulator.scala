package com.adrien.sql

import org.apache.spark.util.AccumulatorV2

class UDAFwithAccumulator extends AccumulatorV2[Int,Int] {
  var sum: Int = 0
  var count: Int = 0

  override def isZero: Boolean ={
    return sum == 0 && count == 0
  }

  override def copy(): AccumulatorV2[Int, Int] = {
    val newUwA = new UDAFwithAccumulator
    newUwA.sum = this.sum
    newUwA.count = this.count
    newUwA
  }

  override def reset(): Unit = {
    sum = 0
    count = 0
  }

  override def add(v: Int): Unit = {
    sum += v
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case o:UDAFwithAccumulator=> {
        sum += o.sum
        count += o.count
      }
      case  _=>
    }
  }

  override def value: Int = sum/count
}

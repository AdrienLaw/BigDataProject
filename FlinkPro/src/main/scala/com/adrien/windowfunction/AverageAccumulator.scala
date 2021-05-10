package com.adrien.windowfunction

import com.adrien.window.Record

import scala.collection.mutable.ArrayBuffer

// 累加器,保存计算过程中的聚合状态
class AverageAccumulator {
  var records = ArrayBuffer[Record]()
  var count:Long = 0L
  var sum:Long = 0L
}

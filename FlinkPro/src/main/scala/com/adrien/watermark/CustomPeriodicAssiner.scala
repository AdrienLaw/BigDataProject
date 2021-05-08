package com.adrien.watermark

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class CustomPeriodicAssiner extends AssignerWithPeriodicWatermarks[Obj5]{
  /** 延迟时间为 1 分钟 */
  val bound: Long = 60 * 1000L

  /** 观察到的最大时间戳 */
  var maxTs: Long = Long.MinValue

  /** 生成当前的 WaterMark */
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  /**
   * 抽取时间戳的方法
   * @param timeStamp 数据
   * @param previousElementTimestamp
   * @return
   */
  override def extractTimestamp(timeStamp: Obj5, previousElementTimestamp: Long): Long = {
    maxTs = previousElementTimestamp.max(timeStamp.time)
    timeStamp.time
  }
}

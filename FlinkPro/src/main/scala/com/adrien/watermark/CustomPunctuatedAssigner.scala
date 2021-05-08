package com.adrien.watermark

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class CustomPunctuatedAssigner extends AssignerWithPunctuatedWatermarks[Obj6]{
  /** 观察到的最大时间戳 */
  val bound: Long = 60 * 1000

  override def checkAndGetNextWatermark(lastElement: Obj6, extractedTS: Long): Watermark = {
    if (lastElement != null) {
      new Watermark(extractedTS - bound)
    } else {
      null
    }
  }

  override def extractTimestamp(lastElement: Obj6, extractedTS: Long): Long = {
    lastElement.time
  }
}

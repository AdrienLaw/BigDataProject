package com.adrien.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkStreamingJoin")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,batchDuration = Seconds(2))
    val leftData = sc.parallelize(0 to 3)
    val rightData = sc.parallelize(0 to 2)
    val leftStream = new ConstantInputDStream(ssc,leftData)
    val rightStream = new ConstantInputDStream(ssc,rightData)
    // 连接的DStream窗口需要有相同的Duration或者其中一个DStream的Duration是另一个的整数倍
    val leftWindow = leftStream.map(f => (f,f)).window(Seconds(2),Seconds(4))
    val rightWindow = rightStream.map(f => (f,f)).window(Seconds(6),Seconds(4))
    leftWindow.join(rightWindow).print()
    ssc.start()
    ssc.awaitTermination()
  }

}

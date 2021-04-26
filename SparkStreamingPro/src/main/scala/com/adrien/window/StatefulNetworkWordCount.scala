package com.adrien.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StatefulNetworkWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("StatefulNetworkWordCount")
      .getOrCreate()
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(".")
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    // 该函数定义了状态更新的逻辑
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))

    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

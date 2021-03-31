package com.adrien.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object makeRDD {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("sparkRDD").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val rdd01 = sparkContext.parallelize(
      List(1,2,3,4)
    )
    val rdd02 = sparkContext.makeRDD(
      List(1,2,3,4)
    )
    rdd01.collect().foreach(println)
    rdd02.collect().foreach(println)
    sparkContext.stop()
  }
}

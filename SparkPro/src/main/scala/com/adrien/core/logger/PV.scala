package com.adrien.core.logger

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PV {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PV").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    val data: RDD[String] = sparkContext.textFile("/Users/luohaotian/Downloads/access.log")
    val pv: Long = data.count()
    print(" PV:" + pv)
    sparkContext.stop()
  }
}

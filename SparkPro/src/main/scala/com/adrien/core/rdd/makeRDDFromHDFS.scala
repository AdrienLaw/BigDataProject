package com.adrien.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object makeRDDFromHDFS {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("makeRDDFromHDFS")
    val sparkContext = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sparkContext.textFile("hdfs://hadoop101:9000/LICENSE.txt")
    fileRDD.collect().foreach(print)
    sparkContext.stop()
  }
}

package com.adrien.core.rdd.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("makeRDDFromHDFS")
    val sparkContext = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4))
    val dataRDD01 = dataRDD.map(
      num => {
        num * 2
      }
    )
    val dataRDD02 = dataRDD01.map(
      num => {
        num * 2
      }
    )
    val rdd: RDD[(Int, String)] = sparkContext.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2,
      "b"), (3, "c"), (3, "c")))
    // 统计每种 key 的个数
    val result: collection.Map[Int, Long] = rdd.countByKey()
    println(result)
  }

}

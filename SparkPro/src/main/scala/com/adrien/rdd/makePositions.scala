package com.adrien.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object makePositions {
  def main(args: Array[String]): Unit = {
    //1、构建sparkConf对象 设置application名称
    val sparkConf = new SparkConf().setAppName("makePositions").setMaster("local[2]")

    /**
     * 2 、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
     * 它内部会构建  DAGScheduler和 TaskScheduler 对象
     */
    val sparkContext = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = sparkContext.makeRDD(
      List(1,2,3,4),
      4
    )
    val fileRDD: RDD[String] = sparkContext.textFile(
      "hdfs://hadoop101:9000/LICENSE.txt",
      2
    )
    fileRDD.collect().foreach(print)
    sparkContext.stop()
  }
}

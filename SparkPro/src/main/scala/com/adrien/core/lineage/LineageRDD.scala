package com.adrien.core.lineage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineageRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf() .setAppName("LineageRDD")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.textFile("/Users/luohaotian/Downloads/Jennifer/HelloApp/input/wordCount/wc.txt")
    println(fileRDD.toDebugString)
    println("----------------------")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("----------------------")
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)
    println("----------------------")
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)
    resultRDD.collect()
    sc.stop()
  }
}

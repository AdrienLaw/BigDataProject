package com.adrien.core.lineage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineageRelyRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf() .setAppName("LineageRelyRDD")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.textFile("/Users/luohaotian/Downloads/Jennifer/HelloApp/input/wordCount/wc.txt")
    println(fileRDD.dependencies)
    println("----------------------")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)
    println("----------------------")
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)
    resultRDD.collect()
  }
}

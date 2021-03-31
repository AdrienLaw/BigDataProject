package com.adrien.core.persistent

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDCheckpoint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LineageRelyRDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")
    val lineRDD: RDD[String] = sc.textFile("/Users/luohaotian/Downloads/Jennifer/HelloApp/input/wordCount/wc.txt")
    // 业务逻辑
    val wordRdd: RDD[String] = lineRDD.flatMap(line => line.split(" "))
    val wordOneRdd: RDD[(String,Long)] = wordRdd.map{
      word => {
        (word,System.currentTimeMillis())
      }
    }
    wordOneRdd.cache()
    wordOneRdd.checkpoint()
    wordOneRdd.collect().foreach(println)
  }
}

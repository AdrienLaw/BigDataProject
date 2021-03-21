package com.adrien.logger

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TopN {
  def main(args: Array[String]): Unit = {
    //1、构建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    //2、构建SparkContext
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    //3、读取数据文件
    val data: RDD[String] = sparkContext.textFile("/Users/luohaotian/Downloads/access.log")
    //4、切分每一行，过滤出丢失的字段数据，获取页面地址
    val filterRDD: RDD[String] = data.filter(x=> x.split(" ").length > 10)
    val urlAndOne: RDD[(String,Int)] = filterRDD.map(x=>x.split(" ")(10)).map((_,1))
    //5、相同url出现的1累加
    val result: RDD[(String,Int)] = urlAndOne.reduceByKey(_+_)
    val sortedRDD: RDD[(String,Int)] = result.sortBy(_._2,false)
    val top5: Array[(String,Int)] = sortedRDD.take(5)
    top5.foreach(println)
    sparkContext.stop()
  }
}

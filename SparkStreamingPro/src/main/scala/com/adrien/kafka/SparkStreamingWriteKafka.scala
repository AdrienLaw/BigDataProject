package com.adrien.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingWriteKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingWriteKafka").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 广播KafkaSink

  }

}

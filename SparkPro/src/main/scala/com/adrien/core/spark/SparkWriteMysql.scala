package com.adrien.core.spark

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object SparkWriteMysql {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("spark://hadoop104:7077").setAppName("SparkSQL")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val rdd: RDD[User2] = spark.sparkContext.makeRDD(List(User2("lisi", 20), User2("zs", 30)))
    val ds: Dataset[User2] = rdd.toDS
    //方式1:通用的方式 format指定写出类型
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/spark") .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

  }

}

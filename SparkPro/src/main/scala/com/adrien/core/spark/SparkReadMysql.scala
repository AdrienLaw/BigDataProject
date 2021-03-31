package com.adrien.core.spark

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadMysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("jdbcMySQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //方式 1:通用的 load 方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/spark")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "person")
      .load().show

    println("==================")
    //方式 2:通用的 load 方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://hadoop101:3306/spark?user=root&password=123123",
        "dbtable"->"person","driver"->"com.mysql.jdbc.Driver")).load().show

    println("==================")
    //方式 3:使用 jdbc 方法读取
    val prop: Properties = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123123")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop101:3306/spark","person",prop)
    df.show()
    //释放资源
    spark.stop()
  }
}

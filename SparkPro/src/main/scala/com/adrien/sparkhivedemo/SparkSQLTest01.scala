package com.adrien.sparkhivedemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQLTest01 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.sql("use sparkhive")
    spark.sql("""select * from city_info""").show
  }

}

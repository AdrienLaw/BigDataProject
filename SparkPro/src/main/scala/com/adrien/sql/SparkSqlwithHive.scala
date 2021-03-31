package com.adrien.sql

import org.apache.spark.sql.SparkSession

object SparkSqlwithHive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("sql")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/tmp/hive/warehouse")
      .getOrCreate()

    val rdd = spark.sql("select * from db_bak.emp_bak").cache()
    rdd.show(false)
  }
}

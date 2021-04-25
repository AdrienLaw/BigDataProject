package com.adrien.sql.udf

import org.apache.spark.sql.SparkSession

object WindowRankUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession      .builder().master("local[1]").appName("Spark SQL basic example3")      .getOrCreate()
    import spark.implicits._
    val df = List(
      ("站点1", "2017-01-01", 50),
      ("站点1", "2017-01-02", 45),
      ("站点1", "2017-01-03", 55),
      ("站点2", "2017-01-01", 25),
      ("站点2", "2017-01-02", 29),
      ("站点2", "2017-01-03", 27)
    ).toDF("site", "date", "user_cnt")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val wSpec = Window.partitionBy("site")
        .orderBy("date")
    df.withColumn("cumSum",rank().over(wSpec)).show()
  }
}

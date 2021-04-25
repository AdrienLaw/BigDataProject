package com.adrien.sql.udf

import org.apache.spark.sql.SparkSession

object WindowLagUDF {
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
    //lag(field, n): 就是取从当前字段往前第n个值，这里是取前一行的值
    df.withColumn("prevUserCnt",
      lag(df("user_cnt"), 1).over(wSpec)).show()
  }
}

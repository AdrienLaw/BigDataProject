package com.adrien.taskscheduler


import java.util.concurrent.Executors

import org.apache.spark.sql.{DataFrame, SparkSession}

object MultiJobTest {
  // spark.scheduler.mode=FAIR
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val rdd = spark.sparkContext.textFile("")
    .map(_.split("\\s+"))
      .map(x => (x(0), x(1)))

    val jobExecutor = Executors.newFixedThreadPool(2)

    jobExecutor.execute(new Runnable {
      override def run(): Unit = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "count-pool")
        val cnt = rdd.groupByKey().count()
        println(s"Count: $cnt")
      }
    })

    jobExecutor.execute(new Runnable {
      override def run(): Unit = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "take-pool")
        val data = rdd.sortByKey().take(10)
        println(s"Data Samples: ")
        data.foreach { x => println(x.mkString(", ")) }
      }
    })

    jobExecutor.shutdown()
    while (!jobExecutor.isTerminated) {}
    println("Done!")
  }
}

package com.adrien.sql.udf

import org.apache.spark.sql.expressions.Window


 
object WindowDF {
  def main(args: Array[String]): Unit = {

    val window = Window.partitionBy("name","subject").orderBy("grade")

  }

}

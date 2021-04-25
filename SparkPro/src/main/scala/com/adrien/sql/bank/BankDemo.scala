package com.adrien.sql.bank
import org.apache.spark.sql.types._

import org.apache.spark.sql.{SparkSession}


object BankDemo {

  import org.apache.spark.sql.functions._
  //葡萄牙银行通过电话访问进行市场调查得到数据集，以下为21个字段


  def main(args: Array[String]): Unit = {

//
//    //受访者年龄
//    val age = StructField("age", DataTypes.IntegerType)
//
//    //受访者职业
//    val job = StructField("job", DataTypes.StringType)
//
//    //婚姻状态
//    val marital = StructField("marital", DataTypes.StringType)
//
//    //受教育程度
//    val edu = StructField("edu", DataTypes.StringType)
//
//    //是否信贷违约
//    val credit_default = StructField("credit_default", DataTypes.StringType)
//
//    //是否有房屋贷款
//    val housing = StructField("housing", DataTypes.StringType)
//
//    //是否有个人贷款
//    val loan = StructField("loan", DataTypes.StringType)
//
//    //联系类型（移动电话或座机）
//    val contact = StructField("contact", DataTypes.StringType)
//
//    //当天访谈的月份
//    val month = StructField("month", DataTypes.StringType)
//
//    //当天访谈时间的是星期几
//    val day = StructField("day", DataTypes.StringType)
//
//    //最后一次电话联系持续时间
//    val dur = StructField("dur", DataTypes.DoubleType)
//
//    //此次访谈的电话联系的次数
//    val campaign = StructField("campaign", DataTypes.DoubleType)
//
//    //距离早前访谈最后一次电话联系的天数
//    val pdays = StructField("pdays", DataTypes.DoubleType)
//
//    //早前访谈电话联系的次数
//    val prev = StructField("prev", DataTypes.DoubleType)
//
//    //早前访谈的结果，成功或失败
//    val pout = StructField("pout", DataTypes.StringType)
//
//    //就业变化率（季度指标）
//    val emp_var_rate = StructField("emp_var_rate", DataTypes.DoubleType)
//
//    //消费者物价指数（月度指标）
//    val cons_price_idx = StructField("cons_price_idx", DataTypes.DoubleType)
//
//    //消费者信心指数（月度指标）
//    val cons_conf_idx = StructField("cons_conf_idx", DataTypes.DoubleType)
//
//    //欧元银行间3月拆借率
//    val euribor3m = StructField("euribor3m", DataTypes.DoubleType)
//
//    //员工数量（季度指标）
//    val nr_employed = StructField("nr_employed", DataTypes.DoubleType)
//
//    //目标变量，是否会定期存款
//    val deposit = StructField("deposit", DataTypes.StringType)
//
//     
//
//    val fields = Array(age, job, marital,
//      edu, credit_default, housing,
//      loan, contact, month,
//      day, dur, campaign,
//      pdays, prev, pout,
//      emp_var_rate, cons_price_idx, cons_conf_idx,
//      euribor3m, nr_employed, deposit)
//
//     
//
//    val schema = StructType(fields)
//
//     
//
//    val spark = SparkSession
//      .builder()
//      .appName("data exploration")
//      .master("local")
//      .getOrCreate()
//
//    import spark.implicits._
//
//     
//
//    //该数据集中的记录有些字段没用采集到数据为unknown
//    val df = spark
//      .read
//      .schema(schema)
//      .option("sep", ";")
//      .option("header", true)
//      .csv("./bank/bank-additional-full.csv")
//
//     
//
//    println(df.count())

  }
}

package com.adrien.cep


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

object CepSelectFuncitonFlatTest extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  // 1. 获取数据输入流
  val socketStream: DataStream[String] = env.socketTextStream("localhost",9999)

  private val recordStream: DataStream[Record] = socketStream
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })
  // 2. 定义一个 Pattern
  private val pattern: Pattern[Record, Record] = Pattern
    // 设置一个简单条件，匹配年龄为 20 的记录
    .begin[Record]("start").where(_.age == 20)
    .next("next").where(_.classId == "2")

  // 3. 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](recordStream, pattern)

  // 4. 获取事件序列，得到匹配到的数据
  private val result: DataStream[String] = patternStream.flatSelect(new flatSelectFunction)
  result.print("CepSelectFuncitonFlatTest")
  env.execute()
}

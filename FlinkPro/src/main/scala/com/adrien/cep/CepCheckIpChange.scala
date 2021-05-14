package com.adrien.cep

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object CepCheckIpChange  extends App{

  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  import org.apache.flink.api.scala._
  //1. 添加 socket 数据源
  val sourceStream: DataStream[String] = environment.socketTextStream("localhost",9999)
  // 2. 数据处理
  val keyedStream: KeyedStream[(String, UserLoginInfo), String] = sourceStream.map(
    x => {
      val strings: Array[String] = x.split(",")
      (strings(1), UserLoginInfo(strings(0), strings(1), strings(2), strings(3)))
    }
  ).keyBy(_._1)
  // 3. 定义Pattern,指定相关条件和模型序列
  val pattern: Pattern[(String, UserLoginInfo), (String, UserLoginInfo)] =
    Pattern.begin[(String, UserLoginInfo)]("start").where(x => x._2.username != null)
      // 使用宽松近邻，使用迭代条件，判断 IP 是否有变更
      .followedBy("second").where(new IpChangeIterativeCondition)
      // 可以指定模式在一段时间内有效
      .within(Time.seconds(120))

  // 4. 模式检测，将模式应用到流中
  val patternStream: PatternStream[(String, UserLoginInfo)] = CEP.pattern(keyedStream,pattern)
  // 5. 选取结果
  patternStream.select(new PatternSelectIpChangeDataFunction).print()
  // 6. 开启计算
  environment.execute()
}

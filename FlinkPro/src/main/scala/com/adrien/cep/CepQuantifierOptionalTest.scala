package com.adrien.cep

import java.util
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

object CepQuantifierOptionalTest  extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 1. 获取数据输入流
  val socketStream: DataStream[String] = env.socketTextStream("localhost", 9999)
  private val recordStream: DataStream[Record] = socketStream
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })

  // 2. 定义一个 Pattern
  private val pattern: Pattern[Record, Record] = Pattern
    .begin[Record]("start")
    .where(_.age == 20).times(2).optional
    .next("next").where(_.classId == "2")
  /**
   * 2,xx,100 >
   * CepQuantifierOptionalTest:7> end:Record(2,xx,100)
   * 1,小红1,20
   * 1,小红2,20
   * 2,xx,300 >
CepQuantifierOptionalTest:9> end:Record(2,xx,300)
CepQuantifierOptionalTest:8> start:Record(1,小红1,20),end:Record(2,xx,300)
   */

  // 3. 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](recordStream, pattern)

  // 4. 获取事件序列，得到匹配到的数据
  private val result: DataStream[String] = patternStream.select(
    new PatternSelectFunction[Record, String] {
      override def select(pattern: util.Map[String, util.List[Record]]): String = {
        // 从 map 中根据名称获取对应的事件
        //
        val result:StringBuffer = new StringBuffer()
        var start: Record = null

        // 当只有 next 匹配到的时候也会触发，这是 start 未匹配到为空，需要进行判断
        if (null != pattern.get("start")){
          start = pattern.get("start").iterator().next()
          result
            .append("start:")
            .append(start.toString)
            .append(",")
        }
        val next: Record = pattern.get("next").iterator().next()
        result.append("end:").append(next.toString)
        result.toString
      }
    }
  )
  result.print("CepQuantifierOptionalTest")
  env.execute()
}

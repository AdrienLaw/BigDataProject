package com.adrien.cep

import java.util
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

object CepContiguityNotFollowedByTest extends App{
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
    .begin[Record]("start").where(_.age == 20)
    // 定义严格近邻模式，age 为 20 的记录后跟随 classId 不为 3 的记录才会匹配
    .notFollowedBy("notFollowedBy").where(_.classId == "3")
    .next("next").where(_.classId == "2")
  /**
  1,小红0,20 触发 1
  2,xx,100 触发 1 》
notFollowedBy==null
CepContiguityNotFollowedByTest:6> start:Record(1,小红0,20),next:Record(2,xx,100),

  1,小红1,20
  3,xx,200
  2,xx,300
  1,小红2,20 触发 2
  2,xx,400 》 触发 2
notFollowedBy==null
CepContiguityNotFollowedByTest:7> start:Record(1,小红2,20),next:Record(2,xx,400),
  1,小红3,20
  4,小红3,20 触发 3
  2,xx,20 > 触发 3
notFollowedBy==null
CepContiguityNotFollowedByTest:8> start:Record(4,小红3,20),next:Record(2,xx,20),
   */

  // 3. 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](recordStream, pattern)

  // 4. 获取事件序列，得到匹配到的数据
  private val result: DataStream[String] = patternStream.select(
    new PatternSelectFunction[Record, String] {
      override def select(pattern: util.Map[String, util.List[Record]]): String = {
        // 从 map 中根据名称获取对应的事件
        val result:StringBuffer = new StringBuffer()
        var start: Record = null
        var notFollowedBy: Record = null
        var next: Record = null
        if (null != pattern.get("start")){
          start = pattern.get("start").iterator().next()
          result
            .append("start:")
            .append(start.toString)
            .append(",")
        }
        println("notFollowedBy=="+pattern.get("notFollowedBy"))
        if (null != pattern.get("notFollowedBy")){
          notFollowedBy = pattern.get("notFollowedBy").iterator().next()
          result
            .append("notFollowedBy:")
            .append(notFollowedBy.toString)
            .append(",")
        }
        if (null != pattern.get("next")){
          next = pattern.get("next").iterator().next()
          result
            .append("next:")
            .append(next.toString)
            .append(",")
        }
        result.toString
      }
    }
  )
  result.print("CepContiguityNotFollowedByTest")
  env.execute()
}

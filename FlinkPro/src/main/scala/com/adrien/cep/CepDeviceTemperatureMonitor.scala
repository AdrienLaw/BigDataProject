package com.adrien.cep

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


object CepDeviceTemperatureMonitor {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setParallelism(1)
  // 1. 指定时间类型
  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  environment.setParallelism(1)
  import org.apache.flink.api.scala._

  // 2. 接受数据
  val sourceStream: DataStream[String] = environment.socketTextStream("localhost",9999)

  val deviceStream: KeyedStream[DeviceDetail, String] = sourceStream.map(
    x => {
      val strings: Array[String] = x.split(",")
      DeviceDetail(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
    }
  ).assignAscendingTimestamps(
    x =>{
      format.parse(x.date).getTime
    }
  ).keyBy(x => x.sensorMac)
  // 3. 定义Pattern,指定相关条件和模型序列
  val pattern: Pattern[DeviceDetail, DeviceDetail] =
    Pattern
      .begin[DeviceDetail]("start")
      .where(x =>x.temperature.toInt >= 40)
      .timesOrMore(3).greedy
      .within(Time.minutes(3))
  // 4. 模式检测，将模式应用到流中
  val patternResult: PatternStream[DeviceDetail] = CEP.pattern(deviceStream,pattern)
  // 5. 选取结果
  patternResult.select(new MyPatternResultFunction).print()
  // 6. 启动
  environment.execute("CepDeviceTemperatureMonitor")
}

package com.adrien.cep

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

class CheckIpChangeProcessFunction  extends KeyedProcessFunction[String,(String,UserLogin),(String,ArrayBuffer[UserLogin])] {
  var valueState: ValueState[UserLogin] = _

  override def open(parameters: Configuration): Unit = {
    val valueStateDescriptor = new ValueStateDescriptor[UserLogin]("changeIp", Types.of[UserLogin])
    valueState = getRuntimeContext.getState(valueStateDescriptor)

  }
  /**
   * 解析用户访问信息
   */
  override def processElement(value: (String, UserLogin),
                              ctx: KeyedProcessFunction[String,
                                (String, UserLogin),
                                (String, ArrayBuffer[UserLogin])]#Context,
                              out: Collector[(String, ArrayBuffer[UserLogin])]): Unit = {

  }
}

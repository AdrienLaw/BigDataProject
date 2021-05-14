package com.adrien.cep

import java.util
import org.apache.flink.cep.PatternSelectFunction
import scala.collection.mutable.ArrayBuffer

class PatternSelectIpChangeDataFunction extends PatternSelectFunction[(String,UserLoginInfo),ArrayBuffer[UserLoginInfo]]{
  override def select(
                       map: util.Map[String,
                         util.List[(String, UserLoginInfo)]]): ArrayBuffer[UserLoginInfo] = {
    val array = new ArrayBuffer[UserLoginInfo]()
    // 获取Pattern名称为start的事件
    val prevLogin= map.get("start").iterator()
    array.append(prevLogin.next()._2)
    //获取Pattern名称为second的事件
    val nextLotin = map.get("second").iterator()
    array.append(nextLotin.next()._2)
    array
  }
}

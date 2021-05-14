package com.adrien.cep

import java.util
import org.apache.flink.cep.PatternSelectFunction
import scala.collection.mutable

class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail,(String,mutable.Map[String,String])]{
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): (String,mutable.Map[String,String]) = {
    val startDetails: util.List[DeviceDetail] = pattern.get("start")

    //1.通过对偶元组创建map 映射
    val map = mutable.Map[String, String]()
    var deviceMac:String = ""
    for ( i <- 0 until startDetails.size()){
      val deviceDetail = startDetails.get(i)
      deviceMac = deviceDetail.deviceMac
      map.put(deviceDetail.date,deviceDetail.temperature)
    }
    (deviceMac,map)
  }
}

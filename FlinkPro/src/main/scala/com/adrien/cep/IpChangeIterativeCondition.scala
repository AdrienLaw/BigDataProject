package com.adrien.cep

import java.util
import org.apache.flink.cep.pattern.conditions.IterativeCondition

/**
 * 使用迭代条件，判断 IP 是否有变更
 */
class IpChangeIterativeCondition extends IterativeCondition[(String, UserLoginInfo)]{

  override def filter(
                       thisLogin: (String, UserLoginInfo),
                       ctx: IterativeCondition.Context[(String, UserLoginInfo)]): Boolean = {
    var flag: Boolean = false
    //获取满足前面条件的数据
    val prevLogin: util.Iterator[(String, UserLoginInfo)] = ctx.getEventsForPattern("start").iterator()
    //遍历
    while (prevLogin.hasNext) {
      val tuple: (String, UserLoginInfo) = prevLogin.next()
      //ip不相同
      if (!tuple._2.ip.equals(thisLogin._2.ip)) {
        flag = true
      }
    }
    flag
  }
}

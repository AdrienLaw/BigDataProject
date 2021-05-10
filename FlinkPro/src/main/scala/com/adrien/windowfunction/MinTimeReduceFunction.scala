package com.adrien.windowfunction

import com.adrien.window.Obj1
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * 定义一个 ReduceFunction 比较两个元素的时间大小，将时间比较大的元素返回
 */
class MinTimeReduceFunction extends ReduceFunction[Obj1]{
  override def reduce(t1: Obj1, t2: Obj1): Obj1 = {
    println(s"r1.time=${t1.time},r2.time=${t2.time}")
    if(t1.time > t2.time){
      println(s"bigger is r1.time=${t1.time}")
      t1
    }else{
      println(s"bigger is r2.time=${t2.time}")
      t2
    }
  }
}

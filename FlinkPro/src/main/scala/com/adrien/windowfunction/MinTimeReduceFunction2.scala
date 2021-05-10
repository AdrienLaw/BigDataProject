package com.adrien.windowfunction

import com.adrien.window.Obj1
import org.apache.flink.api.common.functions.ReduceFunction


class MinTimeReduceFunction2 extends ReduceFunction[Obj1]{
  override def reduce(r1: Obj1, r2: Obj1):Obj1 = {
    if(r1.time > r2.time){
      r1
    }else{
      r2
    }
  }
}

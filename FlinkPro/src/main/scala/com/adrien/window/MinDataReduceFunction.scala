package com.adrien.window

import org.apache.flink.api.common.functions.ReduceFunction

class MinDataReduceFunction extends ReduceFunction[Obj1]{
  override def reduce(t1: Obj1, t2: Obj1): Obj1 = {
    if (t1.time > t2.time) {
      t1
    } else {
      t2
    }
  }
}

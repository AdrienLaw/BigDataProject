package com.adrien.processfunction

import com.adrien.window.Obj1
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector


class CustomProcessJoinFunction  extends ProcessJoinFunction[Obj1,Obj1,(String,Obj1,Obj1)]{
  override def processElement(obj: Obj1,
                obj2: Obj1,
                ctx: ProcessJoinFunction[Obj1, Obj1, (String, Obj1, Obj1)]#Context,
   out: Collector[(String, Obj1, Obj1)]): Unit = {
    out.collect((obj.name,obj,obj2))
  }
}

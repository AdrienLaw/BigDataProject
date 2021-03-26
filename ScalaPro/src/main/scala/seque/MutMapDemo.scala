package seque

import scala.collection.mutable

object MutMapDemo {
  def main(args: Array[String]): Unit = {
    val map01 = new mutable.HashMap[String,Int]()
    //向map中添加数据
    map01("spark")=1
    map01 += (("hadoop",2))
    map01.put("storm",3)
    println(map01)

    //从map中移除元素
    map01 -= "storm"
    map01.remove("spark")
    println(map01)
  }
}

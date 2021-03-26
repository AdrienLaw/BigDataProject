package seque

import scala.collection.mutable

object MutSetDemo {
  def main(args: Array[String]): Unit = {
    val set01 = new mutable.HashSet[Int]()
    set01 += 2
    //add等价于+=
    set01.add(4)
    set01 ++= Set(6,8,9)
    println(set01) //
    //删除一个元素
    set01 -= 6
    set01.remove(2)
    println(set01)
  }
}

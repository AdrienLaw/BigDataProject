package seque

import scala.collection.mutable

object ImmutSetDemo {
  def main(args: Array[String]): Unit = {
    val set01 = new mutable.HashSet[Int]()
    //将元素和set1合并生成一个新的set，原有set不变
    val set02 = set01 + 4
    println(set02) //HashSet(4)
    //set中元素不能重复
    val set03 = set01 ++ Set(5,6,7)
    val set00 = Set(7,8,9) ++ set03
    //set中的元素是随机无序的
    println(set00) //HashSet(5, 6, 9, 7, 8)
    println(set00.getClass) //class scala.collection.immutable.HashSet
  }

}

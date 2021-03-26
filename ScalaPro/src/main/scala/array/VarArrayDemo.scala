package array

import scala.collection.mutable.ArrayBuffer

object VarArrayDemo {
  def main(args: Array[String]): Unit = {
    //定义一个空的可变长Int型数组
    val  nums = ArrayBuffer[Int]()
    //在尾端添加元素
    nums += 1
    ////在尾端添加多个元素
    nums +=  (2,3,4,5)
    //使用++=在尾端添加任何集合
    nums ++= Array(6,7,8)

    for (elem <- nums)
      print(s"$elem")
    println()

    //移除最后2元素
    nums.trimEnd(2)

    //在下标2之前插入元素
    nums.insert(2,20)
    nums.insert(2,30)
    for (elem <- nums)
      print(s"$elem ")
    println()

    //从下标2出移除一个或者多个元素
    nums.remove(2)
    nums.remove(2,2)
    //使用增强for循环进行数组遍历
    for (elem <- nums)
      print(s"${elem} ")
    println()
  }
}

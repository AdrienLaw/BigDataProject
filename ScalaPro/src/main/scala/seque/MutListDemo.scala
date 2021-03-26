package seque

import scala.collection.mutable.ListBuffer

object MutListDemo {
  def main(args: Array[String]): Unit = {
    //构建一个可变列表，初始有3个元素1,2,3
    val lst0 = ListBuffer[Int](1,2,3)
    //创建一个空的可变列表
    val lst1 = new ListBuffer[Int]
    //向lst1中追加元素，注意：没有生成新的集合
    lst1+= 4
    lst1.append(5)
    println(lst1)  //ListBuffer(4, 5)
    //将lst1中的元素最近到lst0中， 注意：没有生成新的集合
    lst0 ++= lst1
    println(lst0) //ListBuffer(1, 2, 3, 4, 5)
    //将lst0和lst1合并成一个新的ListBuffer 注意：生成了一个集合
    val lst2 = lst0 ++ lst1
    println(lst2)  //ListBuffer(1, 2, 3, 4, 5, 4, 5)
    //将元素追加到lst0的后面生成一个新的集合
    val lst3 = lst0 :+ 5
    println(lst3)  //ListBuffer(1, 2, 3, 4, 5, 5)
  }
}

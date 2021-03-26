package basic.circ

object ForDemo {
  def main(args: Array[String]): Unit = {
    //每次循环将区间的一个值赋给i
    for (i <- 1 to 10)
      println(i)
    println("----------------------------")
    //for i <-数组
    val arr = Array ("a","b","c")
    for (i <- arr){
      println(i)
    }
    println("----------------------------")
    val s = "Hello!"
    for (i <- 0 until s.length) {
      println(s(i))
    }
    println("----------------------------")
    //高级for循环
    for (i <- 1 to 3 ;j <- 1 to 3 if i != j) {
      print((10*i +j )+ " ")
      println()
    }
    println("----------------------------")
    //for推导式，如果for循环的循环体以yeild开始，则该循环会构建出一个集合或者数组，每次迭代生成其中的一个值。
    val v = for (i <- 1 to 10)yield i*10
    println(v)
  }

}

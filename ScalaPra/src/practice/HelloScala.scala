package practice

object HelloScala {
  def main(args: Array[String]): Unit = {
    //注意下面两种写法的区别！
    //没有new相当于一个定长为1的数组，里面的元素是3。有new相当于初始化一个长度为3的数组，元素值初始均为0。
    val array1 = Array[Int](3)
    val array2 = new Array[Int](3)
    println(array1)
    println(array1.toBuffer)
    println(array2.toBuffer)

  }

}

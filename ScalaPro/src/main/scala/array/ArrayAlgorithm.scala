package array

object ArrayAlgorithm {
  def main(args: Array[String]): Unit = {
    val arr = Array(9,1,2,5,3,4,7,8,6)
    //求和
    val res1 = arr.sum
    println(res1)

    //求最大值
    val res2 = arr.max
    println(res2)

    //排序
    val res3 = arr.sorted
    for(elem <- res3)
      print(elem + " ")
  }
}

package array

object ArrayTransfer {
  def main(args: Array[String]): Unit = {
    //使用for推导式生成一个新的数组
    val arr = Array(1,2,3,4,5,6,7,8,9)
    val res1 = for (elem <- arr)yield 2*elem
    for (elem <- res1)
      print(elem + " ")
    println()

    //对原数组元素过滤后生成一个新的数组
    //将偶数取出乘以10后再生成一个新的数组
    val res2 = for (elem <- arr if elem%2 ==0)yield 2*elem
    for (elem <- res2)
      print(elem + " ")
    println()

    //使用filter和map转换出新的数组
    val res3 = arr.filter(_%2 == 0).map(2 * _)
    for (elem <- res3)
      print(elem + " ")
    println()
  }
}

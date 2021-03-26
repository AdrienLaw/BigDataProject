package mapping

object MappingDemo {
  def main(args: Array[String]): Unit = {
    //定义构建一个可变的map
    val scores = scala.collection.mutable.Map("zhangsan" -> 20,"lisi" -> 21,"wangwu" -> 19)
    //修改map中对应键的值
    scores("lisi") = 100
    scores("zhaoliu") = 500
    scores += ("suqi" -> 60,"qianba" -> 99)
    //移除某个键值对
    scores -= "zhangsan"
    //获取键的集合并遍历
    var res = scores.keySet
    for (elem <- res)
      print(elem + " ")
    println()

    //遍历map
    for ((k,v)<- scores)
      print(k + "--" + v + "  ")
  }
}

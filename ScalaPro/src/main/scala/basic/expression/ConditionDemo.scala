package basic.expression

object ConditionDemo {
  def main(args: Array[String]): Unit = {
    var x = 0
    val y = if (x > 0) 1 else -1
    println(y)

    val z = if (y > 0) "success" else -1
    println(z)
    //如果缺失else，相当于if(x>2) 1 else （）
    val m = if (x > 0) 1
    println(m)

    val n = if (x > 2) 1 else ()
    println(n)

    val k = if (x < 2) 1 else if (x >= -1) -1 else 0
    println(k)
  }
}

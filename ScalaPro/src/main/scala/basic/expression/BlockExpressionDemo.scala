package basic.expression

object BlockExpressionDemo {
  def main(args: Array[String]): Unit = {
    var x = 0
    val res = if (x < 0) {
      -1
    } else if (x >= 1) {
      1
    } else {
      "error"
    }
    println(res)

    val x0 =1
    val y0 =1
    val x1 =2
    val y1 =2
    val distance ={
      val dx = x1 - x0
      val dy = y1 - y0
      Math.sqrt(dx*dx + dy*dy)
    }
    println(distance)

    //块语句，最后一句是赋值语句，值是unit类型的
    var res2= {
      val dx = x1 - x0
      val dy = y1 - y0
      var res = Math.sqrt(dx*dx + dy*dy)
    }
    println(res2)
  }
}

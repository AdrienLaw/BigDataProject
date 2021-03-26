package array

object ArrayDemo {
  def main(args: Array[String]): Unit = {
    //初始化一个长度为8的定长数组，其所有元素均为0
    val arr1 = new Array[Int](8)
    //直接打印定长数组，内容为数组的hashcode值
    println(arr1)
    //将数组转换成数组缓冲，就可以看到原数组中的内容了
    //toBuffer会将数组转换长数组缓冲
    println(arr1.toBuffer)

    /**
     * 注意：如果不使用new获取数组，相当于调用了数组的apply方法，直接为数组赋值
     * 初始化一个长度为1，值为10的定长数组
     */
    val arr2 = new Array[Int](10)
    //输出数组元素值
    println(arr2.toBuffer)

    //定义一个长度为3的定长数组
    val arr3 = Array("hadoop","spark","flink")
    println(arr3(2))

    //包含10个整数的数组，初始化值为0
    val nums = new Array[Int](10)
    for (i <- 0 until nums.length){
      print(s"$i:${nums(i)}")
      println(nums(i))
    }

    val strs = Array[String]("hadoop","spark")
    for (i <- 0 until strs.length)
      print(s"$i:${strs(i)}")
      println()

    strs(0) = "flinkflink"
    for (i <- 0 until strs.length)
      print(s"$i:${strs(i)}")
      println()
  }



}

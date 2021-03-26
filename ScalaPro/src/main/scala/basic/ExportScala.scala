package basic

object ExportScala {
  /**
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val name = "ApacheCN"
    val age = 1
    val url = "www.apache.cn.org"
    println("name=" + name + " age=" + age + " url="+ url)
    printf("name=%s,age=%d,url=%s \n",name,age,url)
    println(s"name=$name,age=$age,url=$url")
  }
}

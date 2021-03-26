package classes

//定义Point类，构造器带有两个参数
class Point (var x:Int,var y:Int){
  //无返回值的类方法
  def move(dx: Int,dy: Int): Unit ={
    x = x + dx
    y = y + dy
  }

  override def toString: String = s"($x,$y)"
  val point1 = new Point(2,3)
  point1.x
  println(point1)


}

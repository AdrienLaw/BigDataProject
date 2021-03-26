package seque

object ImmutListDemo {
  def main(args: Array[String]): Unit = {
    //创建一个不可变的集合
    val lst1 = List(1,2,3)
    //将0插入到lst1的前面生成一个新的List
    val lst2 = 0 :: lst1  //List(1, 2, 3)
    val lst3 = lst1.::(0) //List(1, 2, 3)
    val lst4 = 0 +: lst1  //List(1, 2, 3)
    val lst5 = lst1.+:(0) //List(1, 2, 3)

    //将一个元素添加到lst1的后面产生一个新的集合
    val lst6 = lst1:+ 3   //List(1, 2, 3, 3)
    //将2个list合并成一个新的List
    val lst0 = List(4,5,6)
    val lst7 = lst1 ++ lst0  //List(1, 2, 3, 4, 5, 6)
    //将lst1插入到lst0前面生成一个新的集合
    val lst8 = lst1 ++: lst0 //List(1, 2, 3, 4, 5, 6)
    //将lst0插入到lst1前面生成一个新的集合
    val lst9 = lst1.:::(lst0) //List(4, 5, 6, 1, 2, 3)
  }
}

package com.adrien.executor

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable




class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{
  var map : mutable.Map[String,Long] = mutable.Map()

  // 累加器是否为初始状态
  override def isZero: Boolean = {
    map.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new WordCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(word: String): Unit = {
    /**
     * 查询 map 中是否存在相同的单词
     * 如果有相同的单词，那么单词的数量加 1
     * 如果没有相同的单词，那么在 map 中增加这个单词
     */
    map(word) = map.getOrElse(word,0L) + 1L
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2)(
      (innerMap,kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1,0L) + kv._2
        innerMap
      }
    )
  }

  // 返回累加器的结果 (Out)
  override def value: mutable.Map[String, Long] = ???
}

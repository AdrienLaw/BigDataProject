package com.adrien.rdd

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

object DirectAPI {
  def main(args: Array[String]): Unit = {

    //1.创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //3.定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop104:9092,hadoop105:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //4.读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("spark"), kafkaPara))
    //5.将每条消息的 KV 取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    //6.计算WordCount
    valueDStream.flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _).print()
    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
}

//  def main(args: Array[String]): Unit = {
//    //步骤一：获取配置信息
//    val conf = new SparkConf().setAppName("sparkstreamingoffset").setMaster("local[5]")
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//    val ssc = new StreamingContext(conf,Seconds(5))
//
//    val brokers = "hadoop9092:9092,hadoop104:9092,hadoop105:9092"
//    val topics = "spark,"
//    val groupId = "sparkk" //注意，这个也就是我们的消费者的名字
//
//    val topicsSet = topics.split(",").toSet
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> brokers,
//      "group.id" -> groupId,
//      "fetch.message.max.bytes" -> "209715200",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "enable.auto.commit" -> "false"
//    )
//
//    //步骤二：获取数据源
//    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
//
//    stream.foreachRDD( rdd =>{
//      //步骤三：业务逻辑处理
//      val newRDD: RDD[String] = rdd.map(_.value())
//      newRDD.foreach( line =>{
//        println(line)
//      })
//      //步骤四：提交偏移量信息，把偏移量信息添加到kafka里
//      val offsetRanges  = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//    })
//
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//  }

}

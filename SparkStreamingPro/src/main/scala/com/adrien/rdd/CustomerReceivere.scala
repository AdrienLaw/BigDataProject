package com.adrien.rdd


import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import com.sun.mail.iap.ConnectionException
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver







class CustomerReceivere(host: String,port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging  {
  //最初启动的时候，调用该方法，作用为:读数据并将数据发送给 Spark
  def onStart() {
    // 启动一个线程，开始接收数据
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }
  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }
  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)

      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}

package com.adrien.cep

/**
 * 定义温度信息样例类
 * @param sensorMac 传感器设备mac地址
 * @param deviceMac 检测机器mac地址
 * @param temperature 温度
 * @param dampness 湿度
 * @param pressure 气压
 * @param date 数据产生时间
 */
case class DeviceDetail(
                         sensorMac:String,
                         deviceMac:String,
                         temperature:String,
                         dampness:String,
                         pressure:String,
                         date:String)

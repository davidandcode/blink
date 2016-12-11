package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

/**
  * Created by shengwei.wang on 12/6/16.
  */
class GCSSubPartition(
                       year:String,
                       month:String,
                       day:String,
                       hour:String,
                       minute:String,
                       second:String
                     )  extends  Serializable{
  def getYear:String = year
  def getMonth:String = month
  def getDay:String = day
  def getHour:String = hour
  def getMinute:String = minute
  def getSecond:String = second
}

package com.creditkarma.blink.impl.writer

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
  def getYear = year
  def getMonth = month
  def getDay = day
  def getHour = hour
  def getMinute = minute
  def getSecond = second
}

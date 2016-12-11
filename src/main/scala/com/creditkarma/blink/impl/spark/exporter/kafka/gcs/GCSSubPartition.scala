package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

/**
  * Created by shengwei.wang on 12/6/16.
  */
case class GCSSubPartition(
                       year:String,
                       month:String,
                       day:String
                     )  extends  Serializable{
  def timePartitionPath:String = year + "/" + month + "/" + day
  def this(inputPath:String) = this(inputPath.substring(0,4),inputPath.substring(5,7),inputPath.substring(8,10))
}

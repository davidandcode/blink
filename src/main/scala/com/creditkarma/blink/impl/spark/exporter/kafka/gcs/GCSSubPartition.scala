package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by shengwei.wang on 12/6/16.
  */
case class GCSSubPartition(
                      ts:Long
                     )  extends  Serializable{

  def timePartitionPath:String = {
    val format:SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    format.format(new Date(ts))
  }
}

package com.creditkarma.blink.impl.spark.exporter.kafka

/**
  * Created by yongjia.wang on 12/7/16.
  */
case class WriterClientMeta(records: Long, bytes: Long, complete: Boolean, message: String = "")

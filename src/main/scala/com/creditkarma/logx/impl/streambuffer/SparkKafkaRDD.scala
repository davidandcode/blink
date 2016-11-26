package com.creditkarma.logx.impl.streambuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges

/**
  * Created by yongjia.wang on 11/25/16.
  */
class SparkKafkaRDD[T](override val rdd: RDD[T] with HasOffsetRanges) extends SparkRDD[T](rdd)

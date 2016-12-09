package com.creditkarma.blink.impl.spark.buffer

import com.creditkarma.blink.base.BufferedData
import org.apache.spark.rdd.RDD

/**
  * Created by yongjia.wang on 11/16/16.
  */
class SparkRDD[T](val rdd: RDD[T]) extends BufferedData {}

package com.creditkarma.logx.impl.streamdata

import com.creditkarma.logx.base.StreamData
import org.apache.spark.rdd.RDD

/**
  * Created by yongjia.wang on 11/16/16.
  */
class SparkRDD[T](val rdd: RDD[T]) extends StreamData {}

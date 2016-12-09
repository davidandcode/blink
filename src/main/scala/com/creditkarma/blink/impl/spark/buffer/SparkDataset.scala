package com.creditkarma.blink.impl.spark.buffer

import com.creditkarma.blink.base.BufferedData
import org.apache.spark.sql.Dataset

/**
  * Created by yongjia.wang on 11/16/16.
  */
class SparkDataset[T] (val dataSet: Dataset[T]) extends BufferedData {}

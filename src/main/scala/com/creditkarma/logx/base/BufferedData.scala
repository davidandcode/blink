package com.creditkarma.logx.base
import com.creditkarma.logx.impl.streambuffer._
/**
  * [[BufferedData]] is the abstraction of data during streaming
  * [[SparkRDD]] and [[SparkDataset]] are the ideal implementation of this abstraction
  * They not only supports buffering in scalable fashion, but also supports transformation out of the box
  * Right now, it's unclear what the general interface exactly looks like,
  * so the trait is empty and the implemention would simply expose the RDD (Dataset) object to transformer and writer,
  * since RDD supports distributed transformation and write actions
  */
trait BufferedData {

}

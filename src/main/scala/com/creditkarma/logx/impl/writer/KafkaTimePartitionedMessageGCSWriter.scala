package com.creditkarma.logx.impl.writer

import com.creditkarma.logx.base.Writer
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.transformer.KafkaTimePartitionedMessage
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 11/18/16.
  */
class KafkaTimePartitionedMessageGCSWriter()
  extends Writer[SparkRDD[KafkaTimePartitionedMessage], KafkaCheckpoint, Seq[OffsetRange], Seq[OffsetRange]]{

  //re-use gcs client object if possible
  /**
    *
    * @param data           Data in the buffer to be flushed
    * @param lastCheckpoint writer may need info saved in last checkpoint to determine what to write.
    *                       For example, to guarantee worst case latency and also avoid too many small files,
    *                       writer may flush a kafka topic-partition based on last flush time even the number of records does not meet the threshold.
    * @return The delta successfully written for the purpose of checkpoint. If all data are written, it's the same as delta
    */
  override def write(data: SparkRDD[KafkaTimePartitionedMessage], lastCheckpoint: KafkaCheckpoint): Seq[OffsetRange] = ???

  override def getMetrics(meta: Seq[OffsetRange]): Seq[Map[Any, Any]] = ???
}

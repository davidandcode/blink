package com.creditkarma.logx

import com.creditkarma.logx.base._
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.impl.transformer.{KafkaMessageWithId, KafkaSparkMessageIdTransformer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Utility functions to source to sink pipe with predefined arguments
  */
object Utils {

  def createKafkaSparkFlow[K, V, _]
  (name: String,
   kafkaParams: Map[String, Object],
   writer: Writer[SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange], _],
   checkpointService: CheckpointService[KafkaCheckpoint],
   flushInterval: Long,
   flushSize: Long
  ): LogXCore[SparkRDD[ConsumerRecord[K, V]], SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange]] = {
    val reader = new KafkaSparkRDDReader[K, V](kafkaParams)
    reader.setMaxFetchRecordsPerPartition(flushSize)
    reader.setFlushInterval(flushInterval)
    new LogXCore(
      appName = name,
      reader = reader,
      transformer = new KafkaSparkMessageIdTransformer[K, V](),
      writer = writer,
      checkpointService = checkpointService
    )
  }
}

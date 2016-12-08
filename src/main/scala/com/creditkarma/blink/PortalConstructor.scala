package com.creditkarma.blink

import com.creditkarma.blink.base._
import com.creditkarma.blink.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.blink.impl.streambuffer.SparkRDD
import com.creditkarma.blink.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.blink.impl.transformer.IdentityTransformer
import com.creditkarma.blink.impl.writer.{KafkaPartitionWriter, KafkaSparkExporterWithWorker}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 12/5/16.
  */
object PortalConstructor {
  def noTransform[I <: BufferedData, C <: Checkpoint[Delta, C], Delta]
  (
    id: String, tickTime: Long,
    reader: Reader[I, C, Delta, _],
    writer: Writer[I, C, Delta, _],
    stateTracker: CheckpointService[C]
  ): Portal[I, I, C, Delta] = {
    new Portal(
      id, tickTime, reader, new IdentityTransformer[I], writer, stateTracker)
  }

  val DefaultTickTime = 1000L
  def createKafkaSparkPortalWithSingleThreadedWriter[K, V, P]
  (name: String,
   kafkaParams: Map[String, Object],
   singleThreadPartitionWriter: KafkaPartitionWriter[K, V, P],
   checkpointService: CheckpointService[KafkaCheckpoint],
   flushInterval: Long,
   flushSize: Long,
   instrumentors: Seq[Instrumentor] = Seq.empty
  ): Portal[SparkRDD[ConsumerRecord[K, V]], SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange]] = {
    val reader = new KafkaSparkRDDReader[K, V](kafkaParams)
    reader.setMaxFetchRecordsPerPartition(flushSize)
    reader.setFlushInterval(flushInterval)
    val portal =
    noTransform(
      id = name, tickTime = DefaultTickTime,
      reader = reader,
      writer = new KafkaSparkExporterWithWorker(singleThreadPartitionWriter),
      stateTracker = checkpointService
    )
    instrumentors.foreach(portal.registerInstrumentor)
    portal
  }
}

package com.creditkarma.blink

import com.creditkarma.blink.base._
import com.creditkarma.blink.impl.spark.buffer.SparkRDD
import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorkerWithSubPartition, ExporterWithWorker}
import com.creditkarma.blink.impl.spark.importer.kafka.{KafkaSparkImporter, KafkaTopicFilter}
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import com.creditkarma.blink.impl.spark.transformer.IdentityTransformer
import kafka.consumer.Whitelist
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * PortalConstructor contains helper functions to construct portals of specific class.
  * The [[Portal]] base class, by design, disables inheritance and only has one constructor with complex parameters to create the universe of all portals.
  * It is not easy to use the original constructor, and most the cases, one needs to create more specific portal classes in many places.
  * The helper functions are used directly by the [[com.creditkarma.blink.factory.PortalFactory]] instances.
  */
object PortalConstructor {
  // Portals without transformation
  def noTransform[I <: BufferedData, C <: Checkpoint[Delta, C], Delta]
  (
    id: String, tickTime: Long,
    reader: Importer[I, C, Delta, _],
    writer: Exporter[I, C, Delta, _],
    stateTracker: StateTracker[C]
  ): Portal[I, I, C, Delta] = {
    new Portal(
      id, tickTime, reader, new IdentityTransformer[I], writer, stateTracker)
  }

  val DefaultTickTime = 1000L

  // Portals import from kafka and write to any destination
  def createKafkaSparkPortalWithSingleThreadedWriter[K, V, P]
  (name: String,
   kafkaParams: Map[String, Object],
   singleThreadPartitionWriter: ExportWorkerWithSubPartition[K, V, P],
   checkpointService: StateTracker[KafkaCheckpoint],
   flushInterval: Long,
   flushSize: Long,
   instrumentors: Seq[Instrumentor] = Seq.empty,
   topicFilter: KafkaTopicFilter = new KafkaTopicFilter(Some(Whitelist(".*")), None)
  ): Portal[SparkRDD[ConsumerRecord[K, V]], SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange]] = {
    val excludeInternalTopics = true // always exclude internal topics
    val reader = new KafkaSparkImporter[K, V](kafkaParams, topicFilter, flushSize = flushSize, flushInterval = flushInterval)
    val portal =
    noTransform(
      id = name, tickTime = DefaultTickTime,
      reader = reader,
      writer = new ExporterWithWorker(singleThreadPartitionWriter),
      stateTracker = checkpointService
    )
    instrumentors.foreach(portal.registerInstrumentor)
    portal
  }

}

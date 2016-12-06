package com.creditkarma.logx

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.creditkarma.logx.base.{BufferedData, Checkpoint, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.impl.transformer.{IdentityTransformer, KafkaMessageWithId, KafkaSparkMessageIdTransformer}
import com.creditkarma.logx.impl.writer.{KafkaPartitionWriter, KafkaSparkRDDPartitionedWriter}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Utility functions to source to sink pipe with predefined arguments
  */
object Utils {

  val DefaultTickTime = 1000L

  def createKafkaSparkPortal[K, V]
  (name: String,
   kafkaParams: Map[String, Object],
   writer: Writer[SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange], _],
   checkpointService: CheckpointService[KafkaCheckpoint],
   flushInterval: Long,
   flushSize: Long
  ): Portal[SparkRDD[ConsumerRecord[K, V]], SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange]] = {
    val reader = new KafkaSparkRDDReader[K, V](kafkaParams)
    reader.setMaxFetchRecordsPerPartition(flushSize)
    reader.setFlushInterval(flushInterval)
    new Portal(
      id = name, tickTime = DefaultTickTime,
      reader = reader,
      transformer = new KafkaSparkMessageIdTransformer[K, V](),
      writer = writer,
      stateTracker = checkpointService
    )
  }

  def createKafkaSparkPortalWithSingleThreadedWriter[K, V, P]
  (name: String,
   kafkaParams: Map[String, Object],
   singleThreadPartitionWriter: KafkaPartitionWriter[K, V, P],
   checkpointService: CheckpointService[KafkaCheckpoint],
   flushInterval: Long,
   flushSize: Long
  ): Portal[SparkRDD[ConsumerRecord[K, V]], SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange]] = {
    val reader = new KafkaSparkRDDReader[K, V](kafkaParams)
    reader.setMaxFetchRecordsPerPartition(flushSize)
    reader.setFlushInterval(flushInterval)
    new Portal(
      id = name, tickTime = DefaultTickTime,
      reader = reader,
      transformer = new KafkaSparkMessageIdTransformer[K, V](),
      writer = new KafkaSparkRDDPartitionedWriter(singleThreadPartitionWriter),
      stateTracker = checkpointService
    )
  }

  def createPortal[I <: BufferedData, C <: Checkpoint[Delta, C], Delta]
  (
    id: String, tickTime: Long,
    reader: Reader[I, C, Delta, _],
    writer: Writer[I, C, Delta, _],
    stateTracker: CheckpointService[C]
  ): Portal[I, I, C, Delta] = {
    new Portal(
      id, tickTime, reader, new IdentityTransformer[I], writer, stateTracker)
  }
}


object Serializer {

  def serialize[T <: Serializable](obj: T): Array[Byte] = {
    val byteOut = new ByteArrayOutputStream()
    val objOut = new ObjectOutputStream(byteOut)
    objOut.writeObject(obj)
    objOut.close()
    byteOut.close()
    byteOut.toByteArray
  }

  def deserialize[T <: Serializable](bytes: Array[Byte]): T = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[T]
    byteIn.close()
    objIn.close()
    obj
  }
}

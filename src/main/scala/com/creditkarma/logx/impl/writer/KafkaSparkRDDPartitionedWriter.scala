package com.creditkarma.logx.impl.writer

import com.creditkarma.logx.Serializer
import com.creditkarma.logx.base._
import com.creditkarma.logx.client.{SparkWorkerModule, ClientModuleType}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.{KafkaMetricDimension, KafkaMetricField}
import com.creditkarma.logx.impl.transformer.KafkaMessageWithId
import com.creditkarma.logx.utils.LazyLog
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.util.{Failure, Success, Try}

case class KafkaOutputPartitionMeta[P](partitionInfo: KafkaOutputPartitionInfo[P], clientMeta: WriterClientMeta, minOffset: Long, maxOffset: Long)

case class KafkaTopicPartitionMeta[P](topicPartition: TopicPartition) extends Metric {
  /**
    * The number of partitions under each topicPartition is unknown, and can be very large potentially.
    * Therefore an aggregation needs to be performed to get the compact topicPartition level meta
    * Aggregation is called in the context of SparkRDD groupBy, and there is no need to check duplication
    * @param meta
    */
  def aggregate(meta: KafkaOutputPartitionMeta[P]): Unit = {
    _partitions += 1
    _clientConfirmedRecords += meta.clientMeta.records
    _clientConfirmedBytes += meta.clientMeta.bytes
    if(meta.clientMeta.complete){
      _completedPartitions += 1
      _completedBytes += meta.clientMeta.bytes
      _completedRecords += meta.clientMeta.records
    }
    if(_minOffset == -1 || meta.minOffset < _minOffset){
      _minOffset = meta.minOffset
    }
    if(_maxOffset == -1 || meta.maxOffset > _maxOffset){
      _maxOffset = meta.maxOffset
    }

  }
  private var _minOffset: Long = -1
  private var _maxOffset: Long = -1
  private var _partitions: Long = 0
  private var _completedPartitions: Long = 0
  /**
    * Some client may partially confirm output records and bytes without 100% completion
    */
  private var _clientConfirmedRecords: Long = 0
  private var _clientConfirmedBytes: Long = 0
  private var _completedRecords: Long = 0
  private var _completedBytes: Long = 0

  def clientConfirmedRecords: Long = _clientConfirmedRecords
  def clientConfirmedBytes: Long = _clientConfirmedBytes
  def completedRecords: Long = _completedRecords
  def completedBytes: Long = _completedBytes
  def completedPartitions: Long = _completedPartitions
  def totalPartitions: Long = _partitions
  def allPartitionsCompleted: Boolean = _partitions == _completedPartitions
  def minOffset: Long = _minOffset
  def maxOffset: Long = _maxOffset

  private def dim = KafkaMetricDimension
  private def field = KafkaMetricField

  override def dimensions: Map[Any, Any] = Map(dim.TopicName -> topicPartition.topic, dim.Partition -> topicPartition.partition)
  override def fields: Map[Any, Any] =
    Map(
      field.OutputPartitions -> totalPartitions,
      field.CompletedPartitions -> completedPartitions,
      field.CompletedBytes -> completedBytes,
      field.CompletedRecords -> completedRecords,
      field.ConfirmedBytes -> clientConfirmedBytes,
      field.CompletedRecords -> clientConfirmedRecords,
      field.OutputCompleted -> allPartitionsCompleted
    )
}

class KafkaOutputMeta[P](meta: Seq[KafkaTopicPartitionMeta[P]], readerDelta: Seq[OffsetRange])
  extends WriterMeta[Seq[OffsetRange]]{
  override def metrics: Metrics = new Metrics {
    override def metrics: Iterable[Metric] = {
      meta ++ inconsistentOffsetRanges
    }
  }
  /**
    * Only successful records count. Zero may indicate the sink has serious issues, and the next cycle will wait for the [[com.creditkarma.logx.base.Portal.tickTime]]
    * Positive number indicates the entire flow is functioning and it will attempt the next cycle immediately.
    * @return
    */
  override def outRecords: Long = meta.map(_.completedRecords).sum

  val completedOffsetRanges = meta.filter(_.allPartitionsCompleted).map{
    meta => meta.topicPartition -> OffsetRange(meta.topicPartition, meta.minOffset, meta.maxOffset + 1)
  }.toMap

  val readerCompletedOffsetRanges =
    readerDelta
    .filter{
      osr => completedOffsetRanges.contains(osr.topicPartition())
    }

  private def dim = KafkaMetricDimension
  private def field = KafkaMetricField
  def inconsistentOffsetRanges: Iterable[Metric] = {
    for(
      inOffsetRange <- readerCompletedOffsetRanges;
      outOffsetRange = completedOffsetRanges.get(inOffsetRange.topicPartition()).get if inOffsetRange != outOffsetRange
    ) yield {
      new Metric {
        override def dimensions: Map[Any, Any] = Map(dim.TopicName -> inOffsetRange.topic, dim.Partition -> inOffsetRange.partition)
        override def fields: Map[Any, Any] =
          Map(
            field.InFromOffset -> inOffsetRange.fromOffset, field.InUntilOffset -> inOffsetRange.untilOffset,
            field.OutFromOffset -> outOffsetRange.fromOffset, field.OutUntilOffset -> outOffsetRange.untilOffset)
      }
    }
  }

  /**
    *
    * @return only checkpoint topicPartitions that are fully completed
    */
  override def delta: Option[Seq[OffsetRange]] = Some(readerCompletedOffsetRanges)

}

case class WriterClientMeta(records: Long, bytes: Long, complete: Boolean, message: String = "")

/**
  * This iterator tracks the max and min of kafka message offset for validation purpose
  * As long as the data is iterated by the output client, the offset should be updated accordingly
  * @param data
  */
class KafkaStreamWithOffsetTracking[K, V](data: Iterator[KafkaMessageWithId[K, V]]) extends Iterator[KafkaMessageWithId[K, V]] {
  var minOffset: Long = -1
  var maxOffset: Long = -1

  val itr = data

  override def hasNext: Boolean = itr.hasNext

  override def next(): KafkaMessageWithId[K, V] = {
    val nextMessage = itr.next()
    def offset = nextMessage.kmId.offset
    if (minOffset == -1 || offset < minOffset) {
      minOffset = offset
    }
    if (maxOffset == -1 || offset > maxOffset) {
      maxOffset = offset
    }
    nextMessage
  }

}

trait KafkaPartitionWriter[K, V, P] extends SparkWorkerModule {
  def useSubPartition: Boolean
  def getSubPartition(payload: V): P
  /**
    * For a [[KafkaPartitionWriter]], it's write status is atomic (all or nothing):
    * either the entire partition is successfully written or none.
    * Even if the writer client supports partial writes, it's still treated atomically by the framework.
    *
    * @param topicPartition the full path should have other necessary prefix such as the gcs bucket etc.
    * @param subPartition sub-partition within the kafka topicPartition, such as time based partition
    * @param data stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    */
  def write(topicPartition: TopicPartition, firstOffset: Long, subPartition: Option[P], data: Iterator[KafkaMessageWithId[K, V]]): WriterClientMeta

  final def writeAndExtractOffset
  (topicPartition: TopicPartition, firstOffset: Long, subPartition: Option[P], data: Iterator[KafkaMessageWithId[K, V]]): (WriterClientMeta, Long, Long) = {
    val streamWithOffsetTracker = new KafkaStreamWithOffsetTracking(data)
    val writerMeta = write(topicPartition, firstOffset, subPartition, streamWithOffsetTracker)
    (writerMeta, streamWithOffsetTracker.minOffset, streamWithOffsetTracker.maxOffset)
  }
  override final def moduleType: ClientModuleType.Value = ClientModuleType.SingleThreadWriter
}

/**
  *
  * @param topicPartition
  * @param contentPartition
  */
case class KafkaOutputPartitionInfo[P](topicPartition: TopicPartition, contentPartition: Option[P]){
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
}

/**
  * [[KafkaSparkRDDPartitionedWriter]] is the closure context of spark lambda, in order to supply [[partitionedWriter]], it must be serializable
  * It's important to keep its serialized size small. The potentially largest object is the checkpoint, for Kafka it includes all topic-partition offsetRange and its first checkpoint time
  * The collected metrics are for each topic-partition, and should be scalable
  * If there are too many topic-partitions to fit in memory, the reader should control it.
  * GroupBy is an expensive operation since it reshuffle the input records without local combining.
  * In this case, even groupBy is simply performing a sub-partitioning of the original map partitions (one for each kafka topicPartition),
  * there is no guarantee Spark can make arrangement of the executors to take advantage of it.
  * This kind of re-shuffling is unavoidable in general since the number of subpartitions may be very large,
  * and they have to be encapsulated as individual tasks to ensure scalability.
  * The alternative is to use mapPartition and create multiple streams on the fly for each sub-partition, and perform synchronized blocking iteration
  * across the subpartitions. The only concern is the number of sub-partitions can be too big to fit in memory, since all the subpartitions must present
  * in memory before completiong the mapPartition.
  * @tparam K
  * @tparam V
  */
class KafkaSparkRDDPartitionedWriter[K, V, P]
(partitionedWriter: KafkaPartitionWriter[K, V, P])
  extends Writer[SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaOutputMeta[P]] {

  override def write(data: SparkRDD[KafkaMessageWithId[K, V]], lastCheckpoint: KafkaCheckpoint, inTime: Long, inDelta: Seq[OffsetRange]): KafkaOutputMeta[P] = {
    // Using local variable will detach the class from serialzied task's closure and avoid unnecessary serializations.
    val localPartitionedWriter = partitionedWriter
    partitionedWriter.registerPortal(portalId)
    val topicPartitionMeta: Seq[KafkaTopicPartitionMeta[P]] =
    if(partitionedWriter.useSubPartition){
      data.rdd.groupBy{
          message => (message.topicPartition, message.batchFirstOffset, localPartitionedWriter.getSubPartition(message.value))
        }.map{ // first write each atomic partition and collect meta
        case ((topicPartition, batchFirstOffset, subPartition), messages) =>
          /**
            * There is no guarantee of message arriving ordering in general, even there is the sub-partition strucutre.
            * Use batchFirstOffset of the topic partition to guarantee idempotent output.
            */
          val partitionInfo = KafkaOutputPartitionInfo(topicPartition, Some(subPartition))
          Try(
            localPartitionedWriter
              .writeAndExtractOffset(topicPartition, batchFirstOffset, Some(subPartition), messages.iterator)) match {
            case Success((clientMeta, minOffset, maxOffset)) => KafkaOutputPartitionMeta(partitionInfo, clientMeta, minOffset, maxOffset)
            case Failure(f) => KafkaOutputPartitionMeta(partitionInfo, WriterClientMeta(0, 0, false, f.getMessage), -1, -1)
          }
      } // then collect topic-partition level meta
        .groupBy(_.partitionInfo.topicPartition).map{
        case(topicPartition, outputMeta) =>
          val topicPartitionMeta = KafkaTopicPartitionMeta[P](topicPartition)
          outputMeta.foreach(topicPartitionMeta.aggregate)
          topicPartitionMeta
      }.collect()
    }
    else{
      /**
        * MapPartition only pass through the data as iterator once without extra buffering.
        */
      data.rdd.mapPartitions {
        case messageIterator =>
          val firstMessage = messageIterator.next()
          val topicPartition = firstMessage.topicPartition
          val firstOffset = firstMessage.offset
          val partitionInfo = KafkaOutputPartitionInfo[P](topicPartition, None)
          val outputMeta =
            Try(
              localPartitionedWriter
                .writeAndExtractOffset(topicPartition, firstOffset, None, Iterator(firstMessage) ++ messageIterator))match {
              case Success((clientMeta, minOffset, maxOffset)) => KafkaOutputPartitionMeta(partitionInfo, clientMeta, minOffset, maxOffset)
              case Failure(f) => KafkaOutputPartitionMeta(partitionInfo, WriterClientMeta(0, 0, false, f.getMessage), -1, -1)
            }
          val topicPartitionMeta = KafkaTopicPartitionMeta[P](topicPartition)
          topicPartitionMeta.aggregate(outputMeta)
          Seq(topicPartitionMeta).iterator
      }.collect()
    }

    val outputMeta = new KafkaOutputMeta(topicPartitionMeta, inDelta)
    if(outputMeta.inconsistentOffsetRanges.nonEmpty){
      updateStatus(new StatusUnexpected(new Exception("Input and output offset ranges does not match"),
        s"Input and output offset ranges does not match ${outputMeta.inconsistentOffsetRanges.map{
          m => s"[d=${m.dimensions},f=${m.fields}]"
        }.mkString(",")}"))
    }
    outputMeta
  }
}

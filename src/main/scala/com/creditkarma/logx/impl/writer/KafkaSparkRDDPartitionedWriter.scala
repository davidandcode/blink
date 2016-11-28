package com.creditkarma.logx.impl.writer

import com.creditkarma.logx.base.{Metric, Metrics, Writer, WriterMeta}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.{KafkaMetricDimension, KafkaMetricField}
import com.creditkarma.logx.impl.transformer.KafkaMessageWithId
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.util.{Failure, Success, Try}

case class KafkaOutputPartitionMeta(partitionInfo: KafkaOutputPartitionInfo, clientMeta: WriterClientMeta, minOffset: Long, maxOffset: Long)

case class KafkaTopicPartitionMeta(topicPartition: TopicPartition) extends Metric {
  /**
    * The number of partitions under each topicPartition is unknown, and can be very large potentially.
    * Therefore an aggregation needs to be performed to get the compact topicPartition level meta
    * Aggregation is called in the context of SparkRDD groupBy, and there is no need to check duplication
    * @param meta
    */
  def aggregate(meta: KafkaOutputPartitionMeta): Unit = {
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
  def minOffset = _minOffset
  def maxOffset = _maxOffset

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

class KafkaOutputMeta(meta: Seq[KafkaTopicPartitionMeta], readerDelta: Seq[OffsetRange])
  extends WriterMeta[Seq[OffsetRange]]{
  override def metrics: Metrics = new Metrics {
    override def metrics: Iterable[Metric] = {
      meta ++ inconsistentOffsetRanges
    }
  }
  /**
    * Only successful records count. Zero may indicate the sink has serious issues, and the next cycle will wait for the [[com.creditkarma.logx.base.LogXCore.tickTime]]
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

case class WriterClientMeta(records: Long, bytes: Long, complete: Boolean)

/**
  * This iterator tracks the max and min of kafka message offset for validation purpose
  * As long as the data is iterated by the output client, the offset should be updated accordingly
  * @param data
  * @tparam K
  * @tparam V
  */
class KafkaStreamWithOffsetTracking[K, V](data: Iterable[KafkaMessageWithId[K, V]]) extends Iterable[KafkaMessageWithId[K, V]]{
  var minOffset: Long = -1
  var maxOffset: Long = -1
  override def iterator: Iterator[KafkaMessageWithId[K, V]] =
    new Iterator[KafkaMessageWithId[K, V]] {
      val itr = data.iterator
      override def hasNext: Boolean = itr.hasNext
      override def next(): KafkaMessageWithId[K, V] = {
        val nextMessage = itr.next()
        def offset = nextMessage.kmId.offset
        if(minOffset == -1 || offset < minOffset){
          minOffset = offset
        }
        if(maxOffset == -1 || offset > maxOffset){
          maxOffset = offset
        }
        nextMessage
      }
    }
}
trait KafkaMessageOutputClientCreator[K, V] extends Serializable {
  def createClient(): KafkaMessageOutputClient[K, V]
}
trait KafkaMessageOutputClient[K, V] {
  /**
    * For a [[KafkaMessageOutputClient]], it's write status is atomic (all or nothing):
    * either the entire partition is successfully written or none.
    * Even if the writer client supports partial writes, it's still treated atomically by the framework.
    * This is the simplest way to define contract between client and the framework
    *
    * @param partition the full path should have other necessary prefix such as the gcs bucket etc.
    * @param data stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    */
  def write(partition: String, data: Iterable[KafkaMessageWithId[K, V]]): WriterClientMeta

}

trait MessagePartitioner[K, V] extends Serializable {
  def contentBasedPartition(payload: V): Option[String]
  // this is the default rule, which can be modified
  def partitionInfo(kafkaMessage: KafkaMessageWithId[K, V]): KafkaOutputPartitionInfo =
    KafkaOutputPartitionInfo(
      kafkaMessage.kmId.topicPartition,
      Try(contentBasedPartition(kafkaMessage.value)) match {
        case Success(p) => p
        case Failure(f) => Some("_failed_content_partition")
      }
    )
}

/**
  * Store all partition information and generate [[finalPartition]] as part of the output path
  * @param topicPartition
  * @param contentPartition
  */
case class KafkaOutputPartitionInfo(topicPartition: TopicPartition, contentPartition: Option[String]){
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
  // Can make it take a custom method to construct the final partition path
  def finalPartition: String = contentPartition match {
    case Some(p) => s"${topic}/${p}/${partition}"
    case None => s"${topic}/${partition}"
  }
}

/**
  * [[KafkaSparkRDDPartitionedWriter]] is the closure context of spark lambda, in order to supply [[partitioner]] and [[writerCreator]], so it must be serializable
  * @param partitioner
  * @param writerCreator
  * @tparam K
  * @tparam V
  */
class KafkaSparkRDDPartitionedWriter[K, V]
(partitioner: MessagePartitioner[K, V], writerCreator: KafkaMessageOutputClientCreator[K, V])
  extends Writer[SparkRDD[KafkaMessageWithId[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaOutputMeta] with Serializable {
  override def write(data: SparkRDD[KafkaMessageWithId[K, V]], lastCheckpoint: KafkaCheckpoint, inTime: Long, inDelta: Seq[OffsetRange]): KafkaOutputMeta = {
    /**
      * The collected metrics are for each topic-partition, and should be manageable
      * If there are too many topic-partitions to fit in memory, the reader should control it to begin with.
      */
    val topicPartitionMeta =
    data.rdd
      .groupBy(partitioner.partitionInfo).map{ // first write each atomic partition and collect meta
      case (partition, messages) =>
        // have to pass all through messages once to find the minOffset, this may cause disk spill
        var minOffset: Long = -1
        var maxOffset: Long = -1
        for(m <- messages){
          if(minOffset == -1 || m.offset < minOffset){
            minOffset = m.offset
          }
          if(maxOffset == -1 || m.offset > maxOffset){
            maxOffset = m.offset
          }
        }
        Try(
          writerCreator.createClient()
            .write(s"${partition.finalPartition}_${minOffset}", messages)) match {
          case Success(clientMeta) => KafkaOutputPartitionMeta(partition, clientMeta, minOffset, maxOffset)
          case Failure(f) => KafkaOutputPartitionMeta(partition, WriterClientMeta(0, 0, false), -1, -1)
        }
    } // then collect topic-partition level meta
      .groupBy(_.partitionInfo.topicPartition).map{
      case(topicPartition, outputMeta) =>
        val topicPartitionMeta = KafkaTopicPartitionMeta(topicPartition)
        outputMeta.foreach(topicPartitionMeta.aggregate)
        topicPartitionMeta
    }.collect()
    new KafkaOutputMeta(topicPartitionMeta, inDelta)
  }
}

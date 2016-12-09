package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.base._
import com.creditkarma.blink.impl.spark.importer.kafka.{KafkaMetricDimension, KafkaMetricField}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

case class KafkaTopicPartitionMeta[P](val offsetRange: OffsetRange) extends Metric {
  def topicPartition: TopicPartition = offsetRange.topicPartition()
  def topic: String = offsetRange.topic
  def partition: Int = offsetRange.partition
  /**
    * The number of partitions under each topicPartition is unknown, and can be very large potentially.
    * Therefore an aggregation needs to be performed to get the compact topicPartition level meta
    * Aggregation is called in the context of SparkRDD groupBy, and there is no need to check duplication
    * @param meta
    */
  def aggregate(meta: KafkaSubPartitionMeta[P]): Unit = {
    _partitions += 1
    _clientConfirmedRecords += meta.clientMeta.records
    _clientConfirmedBytes += meta.clientMeta.bytes
    if(meta.clientMeta.complete){
      _completedPartitions += 1
      _completedBytes += meta.clientMeta.bytes
      _completedRecords += meta.clientMeta.records
    }

  }
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

  private def dim = KafkaMetricDimension
  private def field = KafkaMetricField

  override def dimensions: Map[Any, Any] = Map(dim.TopicName -> topic, dim.Partition -> partition)
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








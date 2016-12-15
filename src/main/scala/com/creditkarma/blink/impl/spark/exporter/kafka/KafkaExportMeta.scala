package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.base.{Metric, Metrics, ExportMeta}
import com.creditkarma.blink.base.Portal
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 12/7/16.
  */
class KafkaExportMeta(meta: Seq[TopicPartitionMeta]) extends ExportMeta[Seq[OffsetRange]]{
  override def metrics: Iterable[Metric] = meta

  /**
    * Only successful records count. Zero may indicate the sink has serious issues, and the next cycle will wait for the [[Portal.tickTime]]
    * Positive number indicates the entire flow is functioning and it will attempt the next cycle immediately.
    * @return
    */
  override def outRecords: Long = completedTopicPartitions.map(_.completedRecords).sum

  override def inRecords: Long = allOffsetRanges.map(_.count()).sum

  /**
    *
    * @return only checkpoint topicPartitions that are fully completed
    */
  override def delta: Option[Seq[OffsetRange]] = Some(completedOffsetRanges)


  def completedTopicPartitions: Seq[TopicPartitionMeta] = meta.filter(_.allPartitionsCompleted)
  //TODO, integration test to make sure partially completed partitions are not checkpointed
  def completedOffsetRanges: Seq[OffsetRange] = completedTopicPartitions.map(_.offsetRange)

  def allOffsetRanges: Seq[OffsetRange] = meta.map(_.offsetRange)

  def checkConsistency(): Unit = {
    val inconsistentTopicPartitions = completedTopicPartitions.filter{
      meta =>
        // offsetRange is what being read. completedRecords come from the writer confirmation.
        // if they do not match, there could be bug or wrong assumptions about consumer reads,
        // and this is serious problem
        meta.offsetRange.count() != meta.completedRecords
    }
    if(inconsistentTopicPartitions.nonEmpty){
      throw new Exception(s"Kafka read and write inconsistent: ${inconsistentTopicPartitions.mkString(",")}")
    }
  }
}

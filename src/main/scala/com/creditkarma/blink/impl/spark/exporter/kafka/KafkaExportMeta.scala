package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.base.{Metric, Metrics, ExportMeta}
import com.creditkarma.blink.base.Portal
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 12/7/16.
  */
class KafkaExportMeta(meta: Seq[TopicPartitionMeta]) extends ExportMeta[Seq[OffsetRange]]{
  override def metrics: Metrics = new Metrics {
    override def metrics: Iterable[Metric] = meta
  }
  /**
    * Only successful records count. Zero may indicate the sink has serious issues, and the next cycle will wait for the [[Portal.tickTime]]
    * Positive number indicates the entire flow is functioning and it will attempt the next cycle immediately.
    * @return
    */
  override def outRecords: Long = meta.map(_.completedRecords).sum

  //TODO, integration test to make sure partially completed partitions are not checkpointed
  private def completedOffsetRanges = meta.filter(_.allPartitionsCompleted).map(_.offsetRange)
  /**
    *
    * @return only checkpoint topicPartitions that are fully completed
    */
  override def delta: Option[Seq[OffsetRange]] = Some(completedOffsetRanges)

}

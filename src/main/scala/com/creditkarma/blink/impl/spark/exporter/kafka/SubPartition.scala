package com.creditkarma.blink.impl.spark.exporter.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Optionally subPartition a Kafka topicPartition during a batch process
  */
case class SubPartition[P](osr: OffsetRange, subPartition: Option[P]){
  def topicPartition: TopicPartition = osr.topicPartition()
  def topic: String = osr.topic
  def partition: Int = osr.partition
  def fromOffset: Long = osr.fromOffset
  def untilOffset: Long = osr.untilOffset
}

case class SubPartitionMeta[P](subPartition: SubPartition[P], workerMeta: WorkerMeta)

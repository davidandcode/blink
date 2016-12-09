package com.creditkarma.blink.impl.spark.exporter.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 12/5/16.
  */
case class KafkaSubPartition[P](osr: OffsetRange, subPartition: Option[P]){
  def topicPartition: TopicPartition = osr.topicPartition()
  def topic: String = osr.topic
  def partition: Int = osr.partition
  def fromOffset: Long = osr.fromOffset
  def untilOffset: Long = osr.untilOffset
}

package com.creditkarma.blink.impl.spark.exporter.kafka

import org.apache.kafka.common.TopicPartition

/**
  * This combination of Ids guarantees uniqueness within a Kafka cluster instance.
  * For safety, a Kafka cluster instance Id can be augmented later by the [[ExportWorkerWithSubPartition]]
  * Kafka cluster Id is not added here because it belongs to the outside context, not here.
  *
  * @param topicPartition
  * @param offset
  */
case class KafkaMessageId(topicPartition: TopicPartition, offset: Long)

case class KafkaMessageWithId[K, V](key: K, value: V, kmId: KafkaMessageId){
  def offset: Long = kmId.offset
  def topicPartition: TopicPartition = kmId.topicPartition
}

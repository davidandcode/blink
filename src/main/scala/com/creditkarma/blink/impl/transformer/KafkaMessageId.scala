package com.creditkarma.blink.impl.transformer

import org.apache.kafka.common.TopicPartition

/**
  * This combination of Ids guarantees uniqueness within a Kafka cluster instance.
  * For safety, a Kafka cluster instance Id can be appended later
 *
  * @param topicPartition
  * @param offset
  */
case class KafkaMessageId(topicPartition: TopicPartition, offset: Long)

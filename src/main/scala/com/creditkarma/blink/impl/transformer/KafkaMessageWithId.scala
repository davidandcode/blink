package com.creditkarma.blink.impl.transformer

import org.apache.kafka.common.TopicPartition

/**
  * Created by yongjia.wang on 12/7/16.
  */
case class KafkaMessageWithId[K, V](key: K, value: V, kmId: KafkaMessageId){
  def offset: Long = kmId.offset
  def topicPartition: TopicPartition = kmId.topicPartition
}

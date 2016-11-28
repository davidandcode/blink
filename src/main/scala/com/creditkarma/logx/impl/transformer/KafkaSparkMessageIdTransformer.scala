package com.creditkarma.logx.impl.transformer

import com.creditkarma.logx.base.Transformer
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.util.{Failure, Success, Try}

/**
  * This combination of Ids guarantees uniqueness within a Kafka cluster instance.
  * For safety, a Kafka cluster instance Id can be appended later
  * @param topicPartition
  * @param offset
  */
case class KafkaMessageId(topicPartition: TopicPartition, offset: Long)
case class KafkaMessageWithId[K, V](key: K, value: V, kmId: KafkaMessageId){
  def offset: Long = kmId.offset
  def topicPartition: TopicPartition = kmId.topicPartition
}

class KafkaSparkMessageIdTransformer[K, V]
  extends Transformer[SparkRDD[ConsumerRecord[K, V]], SparkRDD[KafkaMessageWithId[K, V]]] {
  /**
    * Assuming the messages in each map partition are iterated in the same order as the original Kafka message offset
    * @param input
    * @return
    */
  override def transform(input: SparkRDD[ConsumerRecord[K, V]]): SparkRDD[KafkaMessageWithId[K, V]] = {
    /**
      * [[HasOffsetRanges]] is a trait of KafkaRDD, but KafkaRDD is spark package private
      * The type cast is not safe, but it's the standard way to do it
      */
    val offsetRangeByIndex = input.rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    new SparkRDD(
      input.rdd.mapPartitionsWithIndex {
        case (partitionIndex: Int, consumerRecords: Iterator[ConsumerRecord[K, V]]) =>
          consumerRecords.zipWithIndex.map {
            case (cr: ConsumerRecord[K, V], messageIndex: Int) =>
              val osr = offsetRangeByIndex(partitionIndex)
              KafkaMessageWithId(cr.key(), cr.value(), KafkaMessageId(osr.topicPartition, osr.fromOffset + messageIndex))
          }
      }
    )
  }
}

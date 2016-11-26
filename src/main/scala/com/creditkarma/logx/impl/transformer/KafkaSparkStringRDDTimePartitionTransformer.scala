package com.creditkarma.logx.impl.transformer

import com.creditkarma.logx.base.Transformer
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 11/18/16.
  */
case class KafkaTimePartitionedMessage(topic: String, partition: Int, timePartition: String, message: String)
trait OffsetRangeByIndex {
  def apply(index: Int): OffsetRange
}
trait TimePartitionParser {
  def getTimePartition(message: String): String
}
class KafkaSparkStringRDDTimePartitionTransformer
(offsetRangeByIndex: OffsetRangeByIndex, timePartitionParser: TimePartitionParser)
  extends Transformer[SparkRDD[ConsumerRecord[String, String]], SparkRDD[KafkaTimePartitionedMessage]] {
  override def transform(input: SparkRDD[ConsumerRecord[String, String]]): SparkRDD[KafkaTimePartitionedMessage] = {
    new SparkRDD(
      input.rdd.mapPartitionsWithIndex{
        case (partitionIndex, consumerRecords) =>
          consumerRecords.map{
            cr =>
              val osr: OffsetRange = offsetRangeByIndex(partitionIndex)
              KafkaTimePartitionedMessage(osr.topic, osr.partition, timePartitionParser.getTimePartition(cr.value()), cr.value())
          }
      }
    )
  }
}

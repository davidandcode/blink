package com.creditkarma.logx.impl.writer

import com.creditkarma.logx.base.{ExporterAccessor, StatusUnexpected, Writer}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.transformer.{KafkaMessageId, KafkaMessageWithId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 12/5/16.
  */
case class KafkaOutputPartition[P](osr: OffsetRange, subPartition: Option[P]){
  def topicPartition: TopicPartition = osr.topicPartition()
  def topic: String = osr.topic
  def partition: Int = osr.partition
  def fromOffset: Long = osr.fromOffset
}
case class KafkaOutputPartitionMeta2[P](partitionInfo: KafkaOutputPartition[P], clientMeta: WriterClientMeta, minOffset: Long, maxOffset: Long)

class KafkaSparkExporterWithWorker[K, V, P]
(partitionedWriter: KafkaPartitionWriter[K, V, P])
  extends Writer[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaOutputMeta[P]] {

  override def write(data: SparkRDD[ConsumerRecord[K, V]], sharedState: ExporterAccessor[KafkaCheckpoint, Seq[OffsetRange]]): KafkaOutputMeta[P] = {
    partitionedWriter.registerPortal(portalId) // portalId is not available at construction time
    val localPartitionedWriter = partitionedWriter
    val offsetRangeByIndex = data.rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // convert KafkaRDD to a paird RDD of OffsetRange and its message stream
    val topicPartitionStreamRDD: RDD[(OffsetRange, Iterator[KafkaMessageWithId[K, V]])] =
      data.rdd.mapPartitionsWithIndex {
        case (partitionIndex: Int, consumerRecords: Iterator[ConsumerRecord[K, V]]) =>
          val osr = offsetRangeByIndex(partitionIndex)
          val messageIterator =
            consumerRecords.zipWithIndex.map {
              case (cr: ConsumerRecord[K, V], messageIndex: Int) =>
                KafkaMessageWithId(cr.key(), cr.value(), KafkaMessageId(osr.topicPartition, osr.fromOffset + messageIndex), osr.fromOffset)}
          Seq((osr, messageIterator)).iterator
      }
    // Divide the RDD into subPartitions using groupBy, which is an expensive operation requiring shuffling and buffering with worst case linear size.
    // The message becomes Iterable, not Iterator any more, reflecting the buffering. Its iterator is used in output to keep consistent API.
    // If subPartition is not used, it's simply an on-the-fly map transformation, the entire flow is streamlined without shuffling.
    val outputPartitionStreamRDD: RDD[(KafkaOutputPartition[P], Iterator[KafkaMessageWithId[K, V]])] =
      if (localPartitionedWriter.useSubPartition) {
        // when using subpartition, must perform a groupByKey on each records keyed by the (topicPartition, subPartition) combination
        topicPartitionStreamRDD.flatMap {
          case (osr, messageItr: Iterator[KafkaMessageWithId[K, V]]) => messageItr.map {
            message =>
              val subPartition = Some(localPartitionedWriter.getSubPartition(message.value))
              (KafkaOutputPartition[P](osr, subPartition), message)}
        }.groupByKey.map{
          case (partitionInfo, messages: Iterable[KafkaMessageWithId[K, V]]) => (partitionInfo, messages.iterator)}
      }
      else {
        topicPartitionStreamRDD.map {
          case (osr, messageItr) =>(KafkaOutputPartition[P](osr, None), messageItr)}}

    val topicPartitionMeta: Seq[KafkaTopicPartitionMeta[P]] =
      outputPartitionStreamRDD.map{
        case (outputPartition, messageItr) =>
          val outputMeta =
            Try(
              localPartitionedWriter.writeAndExtractOffset(outputPartition.topicPartition, outputPartition.fromOffset, None, messageItr)) match {
              case Success((clientMeta, minOffset, maxOffset)) => KafkaOutputPartitionMeta2(outputPartition, clientMeta, minOffset, maxOffset)
              case Failure(f) => KafkaOutputPartitionMeta2(outputPartition, WriterClientMeta(0, 0, false, f.getMessage), -1, -1)}
          (outputPartition.topicPartition, outputMeta)
      }.groupByKey().map {
        case (topicPartition, metaIterable) =>
          val topicPartitionMeta = KafkaTopicPartitionMeta[P](topicPartition)
          metaIterable.foreach(topicPartitionMeta.aggregate)
          topicPartitionMeta
      }.collect()

    new KafkaOutputMeta(topicPartitionMeta, sharedState.importerDelta)
  }
}

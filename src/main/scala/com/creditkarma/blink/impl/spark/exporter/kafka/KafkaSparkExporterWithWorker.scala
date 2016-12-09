package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.base.{ExporterAccessor, Exporter}
import com.creditkarma.blink.impl.spark.buffer.SparkRDD
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.util.{Failure, Success, Try}




/**
  * GroupBy is an expensive operation since it reshuffle the input records without local combining.
  * In this case, even groupBy is simply performing a sub-partitioning of the original map partitions (one for each kafka topicPartition),
  * there is no guarantee Spark can make arrangement of the executors to take advantage of it.
  * This kind of re-shuffling is unavoidable in general since the number of subpartitions may be very large, therefore not safe to do in memory,
  * and they have to be encapsulated as individual tasks to ensure scalability. Basically groupBy requires linear size buffering in the worst case.
  * @tparam K
  * @tparam V
  * @tparam P
  */
class KafkaSparkExporterWithWorker[K, V, P]
(partitionedWriter: KafkaPartitionWriter[K, V, P])
  extends Exporter[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaExportMeta[P]] {

  override def export(data: SparkRDD[ConsumerRecord[K, V]], sharedState: ExporterAccessor[KafkaCheckpoint, Seq[OffsetRange]]): KafkaExportMeta[P] = {
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
                KafkaMessageWithId(cr.key(), cr.value(), KafkaMessageId(osr.topicPartition, osr.fromOffset + messageIndex))}
          Seq((osr, messageIterator)).iterator
      }
    // Divide the RDD into subPartitions using groupBy, which is an expensive operation requiring shuffling and buffering with worst case linear size.
    // The message becomes Iterable, not Iterator any more, reflecting the buffering. Its iterator is used in output to keep consistent API.
    // If subPartition is not used, it's simply an on-the-fly map transformation, the entire flow is streamlined without shuffling.
    val outputPartitionStreamRDD: RDD[(KafkaSubPartition[P], Iterator[KafkaMessageWithId[K, V]])] =
      if (localPartitionedWriter.useSubPartition) {
        // when using subpartition, must perform a groupByKey on each records keyed by the (topicPartition, subPartition) combination
        topicPartitionStreamRDD.flatMap {
          case (osr, messageItr: Iterator[KafkaMessageWithId[K, V]]) => messageItr.map {
            message =>
              val subPartition = Some(localPartitionedWriter.getSubPartition(message.value))
              (KafkaSubPartition[P](osr, subPartition), message)}
        }.groupByKey.map{
          case (partitionInfo, messages: Iterable[KafkaMessageWithId[K, V]]) => (partitionInfo, messages.iterator)}
      }
      else {
        topicPartitionStreamRDD.map {
          case (osr, messageItr) =>(KafkaSubPartition[P](osr, None), messageItr)}}

    val topicPartitionMeta: Seq[KafkaTopicPartitionMeta[P]] =
      outputPartitionStreamRDD.map{
        case (subPartition, messageItr) =>
          val outputMeta =
            Try(
              localPartitionedWriter.write(subPartition, messageItr)) match {
              case Success(clientMeta) => KafkaSubPartitionMeta(subPartition, clientMeta)
              case Failure(f) => KafkaSubPartitionMeta(subPartition, WriterClientMeta(0, 0, false, f.getMessage))}
          (subPartition.osr, outputMeta)
      }.groupByKey().map {
        case (osr, metaIterable) =>
          val topicPartitionMeta = KafkaTopicPartitionMeta[P](osr)
          metaIterable.foreach(topicPartitionMeta.aggregate)
          topicPartitionMeta
      }.collect()

    new KafkaExportMeta(topicPartitionMeta)
  }
}

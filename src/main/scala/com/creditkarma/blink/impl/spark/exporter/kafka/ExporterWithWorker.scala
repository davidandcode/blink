package com.creditkarma.blink.impl.spark.exporter.kafka

import com.creditkarma.blink.base.{Exporter, ExporterAccessor, StatusFatal}
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
class ExporterWithWorker[K, V, P]
(exportWorker: ExportWorkerWithSubPartition[K, V, P])
  extends Exporter[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaExportMeta] {

  override def export(data: SparkRDD[ConsumerRecord[K, V]], sharedState: ExporterAccessor[KafkaCheckpoint, Seq[OffsetRange]]): KafkaExportMeta = {
    exportWorker.registerPortal(portalId) // portalId is not available at construction time
    val closureWorker = exportWorker // this reassignment is necessary for Spark not to attempt serializing the entire Exporter class

    // convert KafkaRDD to a pair RDD of OffsetRange and its message stream
    val topicPartitionStreamRDD = getTopicPartitionRDD(data.rdd)

    // Divide the RDD into subPartitions using groupBy, which is an expensive operation requiring shuffling and buffering with worst case linear size.
    // The message becomes Iterable, not Iterator any more, reflecting the buffering. Its iterator is used in output to keep consistent API.
    // If subPartition is not used, it's simply an on-the-fly map transformation, the entire flow is streamlined without shuffling.
    val partitionedStreamRDD = getSubPartitionRDD(topicPartitionStreamRDD, closureWorker)

    // collect the topicPartition level meta data back to driver. This action actually triggers the entire operation.
    val topicPartitionMeta: Seq[TopicPartitionMeta] =
      partitionedStreamRDD.map{
        case (subPartition, messageItr) =>
          val outputMeta =
            Try(
              closureWorker.write(subPartition, messageItr)) match {
              case Success(clientMeta) => SubPartitionMeta(subPartition, clientMeta)
              case Failure(f) => SubPartitionMeta(subPartition, WorkerMeta(0, 0, false, f.getMessage))}
          (subPartition.osr, outputMeta)
      }.groupByKey().map {
        case (osr, metaIterable) =>
          val topicPartitionMeta = TopicPartitionMeta(osr)
          metaIterable.foreach(topicPartitionMeta.aggregate)
          topicPartitionMeta
      }.collect()
    val meta = new KafkaExportMeta(topicPartitionMeta)
    Try(meta.checkConsistency()) match {
      case Success(_) =>
      case Failure(f) => updateStatus(new StatusFatal(f, "Meta data inconsistent is fatal"))
    }
    meta
  }

  private def getTopicPartitionRDD(kafkaRDD: RDD[ConsumerRecord[K, V]]): RDD[(OffsetRange, Iterator[KafkaMessageWithId[K, V]])] = {
    val offsetRangeByIndex = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
    kafkaRDD.mapPartitionsWithIndex {
      case (partitionIndex: Int, consumerRecords: Iterator[ConsumerRecord[K, V]]) =>
        val osr = offsetRangeByIndex(partitionIndex)
        val messageIterator =
          consumerRecords.zipWithIndex.map {
            case (cr: ConsumerRecord[K, V], messageIndex: Int) =>
              KafkaMessageWithId(cr.key(), cr.value(), KafkaMessageId(osr.topicPartition, osr.fromOffset + messageIndex))}
        Seq((osr, messageIterator)).iterator
    }
  }
  private def getSubPartitionRDD(
                 topicPartitionStreamRDD: RDD[(OffsetRange, Iterator[KafkaMessageWithId[K, V]])],
                 exportWorker: ExportWorkerWithSubPartition[K, V, P]): RDD[(SubPartition[P], Iterator[KafkaMessageWithId[K, V]])] = {
    if (exportWorker.useSubPartition) {
      // when using subpartition, must perform a groupByKey on each records keyed by the (topicPartition, subPartition) combination
      topicPartitionStreamRDD.flatMap {
        case (osr, messageItr: Iterator[KafkaMessageWithId[K, V]]) => messageItr.map {
          message =>
            val subPartition = Some(exportWorker.getSubPartition(message.value))
            (SubPartition[P](osr, subPartition), message)}
      }.groupByKey.map{
        case (partitionInfo, messages: Iterable[KafkaMessageWithId[K, V]]) => (partitionInfo, messages.iterator)}
    }
    else {
      topicPartitionStreamRDD.map {
        case (osr, messageItr) =>(SubPartition[P](osr, None), messageItr)}}
  }
}

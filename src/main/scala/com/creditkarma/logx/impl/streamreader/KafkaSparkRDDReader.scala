package com.creditkarma.logx.impl.streamreader

import com.creditkarma.logx.base.{Reader, StatusError, StatusOK}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */
class KafkaSparkRDDReader[K, V](val kafkaParams: Map[String, Object])
  extends Reader[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], Seq[OffsetRange]] {

  private val DefaultFlushInterval: Long = 1000
  private val DefaultMaxRecordsPerPartition: Long = 1000

  private var flushInterval: Long = DefaultFlushInterval // default in msec
  private var maxRecordsPerPartition: Long = DefaultMaxRecordsPerPartition // default records

  // configurations be chained through configuration object as opposed to the reader
  def setMaxFetchRecordsPerPartition(n: Long): Unit = {
    maxRecordsPerPartition = Math.max(1, n) // cannot be 0
  }

  def setFlushInterval(t: Long): Unit = {
    flushInterval = t
  }

  def kafkaConsumer: KafkaConsumer[K, V] = {
    _kafkaConsumer match {
      case Some(kc) => kc
      case None =>
        updateStatus(this, new StatusOK(s"Creating Kafka consumer with ${kafkaParams}"))
        Try(
          new KafkaConsumer[K, V](kafkaParams.asJava)
        ) match {
          case Success(kc) =>
            _kafkaConsumer = Some(kc)
            kc
          case Failure(f) =>
            updateStatus(new StatusError(new Exception(s"Failed to create Kafka consumer: ${kafkaParams}", f)))
            throw f
        }
    }
  }

  override def close(): Unit = {
    _kafkaConsumer match {
      case Some(kc) => kc.close()
      case None =>
        updateStatus(new StatusOK(s"Closing kafka consumer in reader $this"))
    }
  }

  override def fetchData(lastFlushTime: Long, checkpoint: KafkaCheckpoint): (SparkRDD[ConsumerRecord[K, V]], Seq[OffsetRange]) = {

    val topicPartitions: Seq[TopicPartition] = kafkaConsumer.listTopics().asScala.filter {
      case (topic: String, _) => topicFilter(topic)
    }.flatMap(_._2.asScala).map {
      pi => new TopicPartition(pi.topic(), pi.partition())
    }.toSeq

    updateStatus(this, new StatusOK(s"Got topic partitions ${topicPartitions}"))

    val checkpointOffsetMap = checkpoint.nextStartingOffset()
    kafkaConsumer.assign(topicPartitions.asJava) // initialize empty partition offset to 0, otherwise it'll through Exception
    kafkaConsumer.seekToBeginning(topicPartitions.asJava)
    val topicPartitionStartingOffsetMap: Map[TopicPartition, Long] =
      topicPartitions.map{
        tp =>
          val earliestOffset = kafkaConsumer.position(tp)
          checkpointOffsetMap.get(tp) match {
            case Some(checkpointOffset) => // the topic partition is checkpointed previously
              if(checkpointOffset < earliestOffset) // some offset is missed from the last checkpoint and what is currently available
                {
                  updateStatus(this, new StatusError(new Exception(s"Missing messages: ${tp}, from $checkpointOffset to $earliestOffset")))
                }

              tp -> checkpointOffset
            case None => // a new topic partition
              tp -> earliestOffset
          }
      }.toMap

    // the end of offset range always have the exclusive semantics (starting offset is inclusive)
    kafkaConsumer.seekToEnd(topicPartitions.asJava)
    val fetchedOffsetRanges =
      topicPartitionStartingOffsetMap.map{
        case (tp: TopicPartition, startingOffset: Long) =>
          val endPosition = kafkaConsumer.position(tp)
          OffsetRange(
            tp, startingOffset,
            Math.min(startingOffset + maxRecordsPerPartition, endPosition)
          )
      }.filter(_.count() > 0).toSeq

    updateStatus(this, new StatusOK(s"Fetched offset ranges: ${fetchedOffsetRanges}"))

    (new SparkRDD[ConsumerRecord[K, V]](
        KafkaUtils.createRDD[K, V](
          SparkContext.getOrCreate(), // spark context
          kafkaParams.asJava,
          fetchedOffsetRanges.toArray, //message ranges
          PreferConsistent // location strategy
        )
      ),
      fetchedOffsetRanges
      )
  }
  /**
    * private internal mutable states
    */
  private var _kafkaConsumer: Option[KafkaConsumer[K, V]] = None

  /**
    * Kafka reader can be configured to read topics with several approach
    * 1. Specific inclusion/exclusion list
    * 2. Regex
    * 3. Filter method
    * A nicer interface can be exposed later to achieve both flexibility and ease-of-use
    * @param topic
    * @return
    */
  private def topicFilter(topic: String): Boolean = {
    topic.indexOf("__consumer_offsets") == -1
  }

  //override def fetchedRecords: Long = if(_fetchedOffsetRanges.isEmpty) 0 else _fetchedOffsetRanges.map(_.count()).sum
  /**
    * This is about streaming flush policy, can be based on data size, time interval or combination
    *
    * @return whether the currently fetched/buffered data should be flushed down the stream
    */
  override def flush(lastFlushTime: Long, meta: Seq[OffsetRange]): Boolean = {
    System.currentTimeMillis() - lastFlushTime >= flushInterval || meta.exists(_.count()>=maxRecordsPerPartition)
  }

  // for this reader, read meta is the same as read delta,
  // but in general meta data is superset of delta
  override def getDelta(meta: Seq[OffsetRange]): Seq[OffsetRange] = meta

  override def getMetrics(meta: Seq[OffsetRange]): Seq[Map[Any, Any]] = {
    Seq(Map("records" -> meta.map(_.count()).sum))
  }
}

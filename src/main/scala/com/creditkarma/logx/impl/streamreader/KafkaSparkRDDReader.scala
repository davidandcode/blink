package com.creditkarma.logx.impl.streamreader

import com.creditkarma.logx.base.{ReadMeta, Reader, StatusError, StatusOK}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class TopicPartitionMeta
(readTime: Long, availableOffsetRange: OffsetRange, checkpointInfo: Option[(OffsetRange, Long)],
 maxRecords: Long, maxInterval: Long){

  val nextOffsetRange: OffsetRange = checkpointInfo match {
    case Some((osr, time)) =>
      val fromOffset = Math.max(osr.untilOffset, availableOffsetRange.fromOffset)
      val untilOffset = Math.min(fromOffset + maxRecords, availableOffsetRange.untilOffset)
      OffsetRange(availableOffsetRange.topicPartition(), fromOffset, untilOffset)
    case None =>
      val fromOffset = availableOffsetRange.fromOffset
      val untilOffset = Math.min(fromOffset + maxRecords, availableOffsetRange.untilOffset)
      OffsetRange(availableOffsetRange.topicPartition(), fromOffset, untilOffset)
  }

  // gap must be reported since it's data lost
  val gap: Long = checkpointInfo match {
    case Some((osr, time)) =>
      Math.max(0, availableOffsetRange.fromOffset - osr.untilOffset)
    case None => 0 // Gap is relative to previous checkpoint, if no checkpoint, there is no gap
  }

  def shouldFlush: Boolean = nextOffsetRange.count() >= maxRecords || {
    checkpointInfo match {
      case Some((offset, time)) => readTime - time >= maxInterval
      case None => true
    }
  }

  def metric: Map[Any, Any] =
    Map(
      "topic" -> availableOffsetRange.topic,
      "partition" -> availableOffsetRange.partition,
      "gap" -> gap,
      "available records" -> availableOffsetRange.count(),
      "flushed records" -> checkpointInfo.map(_._1.count()),
      "records to flush" -> {if(shouldFlush) nextOffsetRange.count() else 0L},
      "read time" -> readTime,
      "checkpoint read time" -> checkpointInfo.map(_._2),
      "max records per partition" -> maxRecords,
      "flush interval" -> maxInterval
    )
}

/**
  *
  * @param readTime Timestamp of the read operation of this cycle
  * @param lastCheckpoint
  * @param offsetRanges Currently available topic partition offsetRanges
  * @param maxRecordsPerPartition It's possible to set topic specific policy
  */
class KafkaSparkReaderMeta
(override val readTime: Long, lastCheckpoint: KafkaCheckpoint, offsetRanges: Seq[OffsetRange],
 maxRecordsPerPartition: Long, maxInterval: Long) extends ReadMeta[Seq[OffsetRange]] {

  val topicPartitionMetaData: Seq[TopicPartitionMeta] = {
    val lastCheckpointMap = lastCheckpoint.timestampedOffsetRanges.map{
      case (osr, time) => osr.topicPartition() -> (osr, time)
    }.toMap
    offsetRanges.map{
      osr => new TopicPartitionMeta(readTime, osr, lastCheckpointMap.get(osr.topicPartition()),
        maxRecordsPerPartition, maxInterval)
    }
  }

  val delta: Seq[OffsetRange] =
    topicPartitionMetaData.iterator
      .filter{
        meta => meta.shouldFlush && meta.nextOffsetRange.count() > 0
      }.map(_.nextOffsetRange).toSeq

  def metrics: Seq[Map[Any, Any]] = topicPartitionMetaData.map(_.metric) :+
    Map[Any, Any]("topics"->totalTopics, "messages"->totalMessages, "dimension"->"all")

  def totalTopics: Int = offsetRanges.map(_.topic).distinct.size

  def totalMessages: Long = offsetRanges.map(_.count()).sum

  override def shouldFlush: Boolean = delta.nonEmpty
}

class KafkaSparkRDDReader[K, V](val kafkaParams: Map[String, Object])
  extends Reader[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaSparkReaderMeta] {

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

  override def fetchData(checkpoint: KafkaCheckpoint): (SparkRDD[ConsumerRecord[K, V]], KafkaSparkReaderMeta) = {

    val readTime = System.currentTimeMillis()
    val topicPartitions: Seq[TopicPartition] = kafkaConsumer.listTopics().asScala.filter {
      case (topic: String, _) => topicFilter(topic)
    }.flatMap(_._2.asScala).map {
      pi => new TopicPartition(pi.topic(), pi.partition())
    }.toSeq

    updateStatus(this, new StatusOK(s"Got topic partitions ${topicPartitions}"))

    kafkaConsumer.assign(topicPartitions.asJava) // initialize empty partition offset to 0, otherwise it'll through Exception
    kafkaConsumer.seekToBeginning(topicPartitions.asJava)
    val topicPartitionStartingOffsetMap: Seq[(TopicPartition, Long)] =
      topicPartitions.map{
        tp => (tp, kafkaConsumer.position(tp))

      }

    // the end of offset range always have the exclusive semantics (starting offset is inclusive)
    kafkaConsumer.seekToEnd(topicPartitions.asJava)
    val availableOffsetRanges: Seq[OffsetRange] =
    topicPartitionStartingOffsetMap.map{
      case (tp, earliestOffset) => OffsetRange(tp, earliestOffset, kafkaConsumer.position(tp))
    }

    val meta = new KafkaSparkReaderMeta(readTime, checkpoint,
      availableOffsetRanges,maxRecordsPerPartition, flushInterval)

    (new SparkRDD[ConsumerRecord[K, V]](
        KafkaUtils.createRDD[K, V](
          SparkContext.getOrCreate(), // spark context
          kafkaParams.asJava,
          meta.delta.toArray, //message ranges
          PreferConsistent // location strategy
        )
      ),
      meta
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

}

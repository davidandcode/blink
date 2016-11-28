package com.creditkarma.logx.impl.streamreader

import com.creditkarma.logx.base._
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


object KafkaMetricDimension extends Enumeration {
  val
  TopicName, Partition,
  FlushInterval, // in ms, each topic may have a different flush interval
  MaxFlushRecords
  = Value
}

object KafkaMetricField extends Enumeration {
  val
  /**
    * Reader metrics
    */
  MessageGap, TimeLag, MessageLag,
  MessagesToPush, MessagesPushed, KafkaRetainedMessages, KafkaLifeTimeMessages,
  /**
    * Writer metrics
    */
  OutputPartitions, CompletedPartitions, CompletedBytes, CompletedRecords, ConfirmedBytes, ConfirmedRecords, OutputCompleted,
  InFromOffset, InUntilOffset, OutFromOffset, OutUntilOffset
  = Value
}

class TopicPartitionMeta
(readTime: Long, availableOffsetRange: OffsetRange, checkpointInfo: Option[(OffsetRange, Long)],
 maxRecords: Long, maxInterval: Long) extends Metric {

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

  val shouldFlush: Boolean = nextOffsetRange.count() >= maxRecords || {
    checkpointInfo match {
      case Some((offset, time)) => readTime - time >= maxInterval
      /**
        * If this is the first time to read from the beginning, and there was no previous checkpoint,
        * the always flush, since there is no way to tell the time latency - unless time of the first message is recorded somewhere
        * If reading from the tail of the message queue, a checkpoint should first be generated recording the offset and the time
        */
      case None => true
    }
  }

  def topic: String = availableOffsetRange.topic
  def partition: Int = availableOffsetRange.partition
  // gap must be reported since it's data lost
  def messageGap: Long = checkpointInfo match {
    case Some((osr, time)) =>
      Math.max(0, availableOffsetRange.fromOffset - osr.untilOffset)
    case None => 0 // Gap is relative to previous checkpoint, if no checkpoint, there is no gap
  }

  /**
    * This is the number of pending messages after this cycle of flush.
    * By design, messages available in Kafka but not flushed to writer is always expected,
    * although those messages should never stay more than [[maxInterval]] ms.
    * @return
    */
  def messageLag: Long = availableOffsetRange.untilOffset -
    {if(shouldFlush) nextOffsetRange.untilOffset else nextOffsetRange.fromOffset}

  /**
    * This is the latency based on read time. Since each read may not flush all the records,
    * one also needs to look at messageLag.
    * For example, if a job starts as back-filling all previous data, the first cycle's timeLag is zero,
    * and the actual lag is reflected by the remaining messages.
    * The cycles following the first back-filling cycle will also have very low time latency,
    * since the cycles run back-to-back without waiting for the [[LogXCore.tickTime]] until there is no more messages to be pushed.
    * By design, the timeLag should never go significantly above [[maxInterval]] + [[LogXCore.tickTime]]
    * @return Time between last checkpoint's cycle read time and the current cycle read time.
    *         Zero is there is no previous checkpoint.
    */
  def timeLag: Long = checkpointInfo.map(readTime - _._2).getOrElse(0L)

  def messagesToFlush: Long = if(shouldFlush) nextOffsetRange.count() else 0L

  def kafkaRetainedMessages: Long = availableOffsetRange.count()

  def kafkaLifeTimeMessages: Long = availableOffsetRange.untilOffset

  def alreadyFlushedMessages: Long = checkpointInfo.map(_._1.count()).getOrElse(0L)

  private def dim = KafkaMetricDimension
  private def field = KafkaMetricField

  override def dimensions: Map[Any, Any] =
    Map(
      dim.TopicName -> topic,
      dim.Partition -> partition,
      dim.FlushInterval -> maxInterval,
      dim.MaxFlushRecords -> maxRecords)

  override def fields: Map[Any, Any] =
    Map(
      field.MessageGap -> messageGap,
      field.MessageLag-> messageLag,
      field.TimeLag -> timeLag,
      field.MessagesToPush -> messagesToFlush,
      field.MessagesPushed -> alreadyFlushedMessages,
      field.KafkaRetainedMessages -> kafkaRetainedMessages,
      field.KafkaLifeTimeMessages -> kafkaLifeTimeMessages
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

  def metrics: Metrics = new Metrics {
    override def metrics: Iterable[Metric] =
      topicPartitionMetaData :+
        new Metric {
          override def dimensions: Map[Any, Any] = Map()
          override def fields: Map[Any, Any] = Map("topics"->totalTopics, "messages"->totalMessages)
        }
  }

  def totalTopics: Int = offsetRanges.map(_.topic).distinct.size

  def totalMessages: Long = offsetRanges.map(_.count()).sum

  override def shouldFlush: Boolean = delta.nonEmpty
}

class KafkaSparkRDDReader[K, V](val kafkaParams: Map[String, Object])
  extends Reader[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], KafkaSparkReaderMeta] {

  private val DefaultFlushInterval: Long = 1000
  private val DefaultMaxRecordsPerPartition: Long = 1000

  private var flushInterval: Long = DefaultFlushInterval // default in msec
  /**
    * Maximum number of records to flush per partition per cycle, extra records will be flushed next cycle
    * This is to limit the maximum size of each output file, when the velocity is high and writer directly map partitions to files with limited control.
    * On the other hand, if the writer has more control over how to write,
    * then reader should always flush all available new data, making [[maxRecordsPerPartition]] unnecessary.
    * The current design is to simply let reader control data flush based on both time and number of records, so that the system
    * can have a well defined guarantee that the data latency is no more than [[maxRecordsPerPartition]] records per partition,
    * and never more than [[flushInterval]] ms in time, for all the topics - per topic level setting is also possible.
    * This can avoid having too many small files at the same time.
    * It may results in a lot of large files, each with [[maxRecordsPerPartition]], when data velocity is high,
    * or limited number of small files with no more than 1 file per [[flushInterval]].
    * It does not provide guarantee in terms of file size on the output side, which depends on many factors and is very hard to control,
    * such as average record size, content-dependent custom output partitioning rules, and compression.
    * Although more sophisticated policies are possible, there is always the tension between data latency
    * and having appropriate batch size/frequency as required by the destination system (sink),
    * as well as the associated patterns of consumption.
    * Therefore, the current design is best in terms of simplicity of the guarantee and good balance among the tensions.
    */
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
    val topicPartitionStartingOffsets: Seq[(TopicPartition, Long)] =
      topicPartitions.map{
        tp => (tp, kafkaConsumer.position(tp))
      }

    // the end of offset range always have the exclusive semantics (starting offset is inclusive)
    kafkaConsumer.seekToEnd(topicPartitions.asJava)
    val availableOffsetRanges: Seq[OffsetRange] =
    topicPartitionStartingOffsets.map{
      case (tp, earliestOffset) => OffsetRange(tp, earliestOffset, kafkaConsumer.position(tp))
    }

    val meta = new KafkaSparkReaderMeta(readTime, checkpoint,
      availableOffsetRanges, maxRecordsPerPartition, flushInterval)

    (
      new SparkRDD[ConsumerRecord[K, V]](
        KafkaUtils.createRDD[K, V](
          SparkContext.getOrCreate(), // spark context
          kafkaParams.asJava,
          meta.delta.toArray, //message ranges
          PreferConsistent // location strategy
        )),
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

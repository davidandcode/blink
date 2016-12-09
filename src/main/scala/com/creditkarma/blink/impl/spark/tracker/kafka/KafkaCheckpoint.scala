package com.creditkarma.blink.impl.spark.tracker.kafka

import com.creditkarma.blink.base.Checkpoint
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * @param timestampedOffsetRanges the complete checkpoint information for Kafka,
  *        including the topic partition offset ranges and the last time it was read
  *        Most likely, the Kafka reader only needs to look at offset ranges, and buffer all the new data
  *        The writer may need to look at the timestamps in order to make sure all topic-partitions,
  *        especially the low velocity ones, are flush at least once a while
  */
class KafkaCheckpoint(val timestampedOffsetRanges: Seq[(OffsetRange, Long)] = Seq.empty)
  extends Checkpoint[Seq[OffsetRange], KafkaCheckpoint]{

  def nextStartingOffset(): Map[TopicPartition, Long] = {
    timestampedOffsetRanges.map{
      case (osr, ts) => osr.topicPartition() -> osr.untilOffset
    }.toMap
  }

  override def toString: String = {
    if(timestampedOffsetRanges.isEmpty){
      "KafkaCheckpoint()"
    }
    else{
      val topicInfoMap: Map[String, TopicSummary] =
        for((topic, topicTsOsr) <- timestampedOffsetRanges.groupBy(_._1.topic))
          yield{
            topic -> new TopicSummary(topic, topicTsOsr)
          }
      val totalTopics = topicInfoMap.size
      val totalPartitions = topicInfoMap.values.map(_.partitions).sum
      val totalRecords = topicInfoMap.values.map(_.lifeTimeRecords).sum
      val topicSummaries = topicInfoMap.values
      val topicWithMostParitions = topicSummaries.maxBy(_.partitions)
      val topicWithRecords = topicSummaries.maxBy(_.lifeTimeRecords)
      val mostFreshTopic = topicSummaries.maxBy(_.mostFreshPartitionTime)
      val mostStaleTopic = topicSummaries.minBy(_.mostFreshPartitionTime)
      val topicTitles = Seq[(String, TopicSummary)](
        "maxp" -> topicWithMostParitions,
        "maxr" -> topicWithRecords,
        "new" -> mostFreshTopic,
        "old" -> mostStaleTopic
      )
      val topicSummaryWithTitles = topicTitles.groupBy(_._2).map{
        case(topicSummary, titles) =>
          def titleStr = s"title(${titles.map(_._1).mkString("/")})"
          s"{$titleStr: $topicSummary}"
      }
      s"KafkaCheckpoint(ts=$totalTopics, ps=$totalPartitions, rs=$totalRecords, champions=${topicSummaryWithTitles.mkString(",")})"
    }
  }

  /**
    *
    * @param delta delta of the source to be merged with the checkpoint
    * @param inTime timestamp in epoch ms of the input read time,
    *               this information can be used for time based flush policy at more granular level
    *               such as per topic partition flush
    * @return
    */
  override def mergeDelta(delta: Seq[OffsetRange], inTime: Long): KafkaCheckpoint = {
    val offsetRangesMap = collection.mutable.Map.empty[TopicPartition, (OffsetRange, Long)] ++
      timestampedOffsetRanges.map{
        case(osr, ts) => osr.topicPartition() -> (osr, ts)
      }.toMap
    for(offsetRange <- delta){
      offsetRangesMap.get(offsetRange.topicPartition()) match {
        case Some((existingOffsetRange, ts)) =>
          offsetRangesMap(offsetRange.topicPartition()) =
            (OffsetRange(offsetRange.topicPartition(), existingOffsetRange.fromOffset, offsetRange.untilOffset), inTime)
        case None =>
          offsetRangesMap += offsetRange.topicPartition()->(offsetRange, inTime)
      }
    }
    new KafkaCheckpoint(offsetRangesMap.values.toSeq)
  }
}

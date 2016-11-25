package com.creditkarma.logx.impl.checkpoint

import com.creditkarma.logx.base.Checkpoint
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * For Kafka, the checkpoint is the end of each offsetRange, which will be used as stating point to construct new offsetRanges
  * The reader is responsible for correctly interpreting the checkpoint, and generating new checkpoint for next batch
  * @param offsetRanges
  */
class KafkaCheckpoint(val offsetRanges: Seq[(OffsetRange, Long)] = Seq.empty) extends Checkpoint[Seq[OffsetRange], KafkaCheckpoint]{

  def nextStartingOffset(): Map[TopicPartition, Long] = {
    offsetRanges.map{
      case (osr, ts) => osr.topicPartition() -> osr.untilOffset
    }.toMap
  }

  override def toString: String = offsetRanges.mkString(", ")

  /**
    *
    * @param delta delta of the source to be merged with the checkpoint
    * @param inTime timestamp in epoch ms of the input read time, this information can be used for time based flush policy at more granular level such as per topic partition flush
    * @return
    */
  override def mergeDelta(delta: Seq[OffsetRange], inTime: Long): KafkaCheckpoint = {
    val offsetRangesMap = collection.mutable.Map.empty[TopicPartition, (OffsetRange, Long)] ++
      offsetRanges.map{
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

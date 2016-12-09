package com.creditkarma.blink.impl.spark.tracker.kafka

import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 12/7/16.
  */
class TopicSummary(topic: String, timestampedOffsetRanges: Seq[(OffsetRange, Long)]){
  val partitions: Int = timestampedOffsetRanges.size
  val mostFreshPartition: (OffsetRange, Long) = timestampedOffsetRanges.maxBy(_._2)
  val mostFreshPartitionTime: Long = mostFreshPartition._2
  val mostFreshPartitionOSR: OffsetRange = mostFreshPartition._1

  val mostStalePartition: (OffsetRange, Long) = timestampedOffsetRanges.minBy(_._2)
  val mostStalePartitionTime: Long = mostStalePartition._2
  val mostStalePartitionOSR: OffsetRange = mostStalePartition._1

  val lifeTimeRecords: Long = timestampedOffsetRanges.map(_._1.count()).sum

  private def osrToString(osr: OffsetRange): String = {
    s"p${osr.partition}:${osr.fromOffset}->${osr.untilOffset}"
  }
  override def toString: String = {
    val currentTime = System.currentTimeMillis()
    def minLatency = currentTime - mostFreshPartitionTime
    def maxLatency = currentTime - mostStalePartitionTime

    s"Topic[$topic, p=$partitions, r=$lifeTimeRecords, " +
      s"latency={range:$minLatency->$maxLatency, fresh:[${osrToString(mostFreshPartitionOSR)},$mostFreshPartitionTime], " +
      s"stale:[${osrToString(mostStalePartitionOSR)}, $mostStalePartitionTime]" +
      s"}]"
  }
}

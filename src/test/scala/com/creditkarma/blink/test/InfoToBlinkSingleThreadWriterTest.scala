package com.creditkarma.blink.test

import com.creditkarma.blink.base.StateTracker
import com.creditkarma.blink.impl.spark.tracker.kafka.{KafkaCheckpoint, ZooKeeperStateTracker}
import com.creditkarma.blink.instrumentation.InfoToKafkaSingleThreadWriter

/**
  * Created by shengwei.wang on 12/8/16.
  */
class InfoToBlinkSingleThreadWriterTest extends BlinkUnitTest{
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: StateTracker[KafkaCheckpoint] = new InMemoryKafkaStateTracker

}
package com.creditkarma.blink.test.itegration

import com.creditkarma.blink.base.StateTracker
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint

/**
  * Created by yongjia.wang on 11/30/16.
  */
class BlinkBasicIntegrationTest extends BlinkKafkaIntegrationTest{
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: StateTracker[KafkaCheckpoint] = new InMemoryKafkaStateTracker
}

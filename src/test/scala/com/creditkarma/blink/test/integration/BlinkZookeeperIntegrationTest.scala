package com.creditkarma.blink.test.integration

import com.creditkarma.blink.base.StateTracker
import com.creditkarma.blink.impl.spark.tracker.kafka.{KafkaCheckpoint, ZooKeeperStateTracker}

/**
  * Created by yongjia.wang on 11/30/16.
  */
class BlinkZookeeperIntegrationTest extends BlinkKafkaIntegrationTest{
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: StateTracker[KafkaCheckpoint] = {
    new ZooKeeperStateTracker("localhost:" + zkPort)
  }
}

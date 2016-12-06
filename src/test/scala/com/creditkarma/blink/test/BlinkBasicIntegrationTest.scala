package com.creditkarma.blink.test
import com.creditkarma.blink.base.CheckpointService
import com.creditkarma.blink.impl.checkpoint.KafkaCheckpoint

/**
  * Created by yongjia.wang on 11/30/16.
  */
class BlinkBasicIntegrationTest extends BlinkKafkaIntegrationTest{
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: CheckpointService[KafkaCheckpoint] = new InMemoryKafkaCheckpointService
}

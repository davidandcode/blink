package com.creditkarma.logx.test
import com.creditkarma.logx.base.CheckpointService
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint

/**
  * Created by yongjia.wang on 11/30/16.
  */
class BlinkBasicIntegrationTest extends BlinkKafkaIntegrationTest{
  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: CheckpointService[KafkaCheckpoint] = new InMemoryKafkaCheckpointService
}

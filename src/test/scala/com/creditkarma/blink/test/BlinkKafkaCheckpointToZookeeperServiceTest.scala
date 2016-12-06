package com.creditkarma.blink.test

import com.creditkarma.blink.base.CheckpointService
import com.creditkarma.blink.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.blink.impl.checkpointservice.ZooKeeperCPService

/**
  * Created by yongjia.wang on 11/30/16.
  */
class BlinkKafkaCheckpointToZookeeperServiceTest extends BlinkKafkaIntegrationTest{

  val myKafkaParam = kafkaParams
  val myZkPort = zkPort

  override def getWriter: WriterType = SimpleCollectibleWriter
  override def getCheckpointService: CheckpointService[KafkaCheckpoint] = new ZooKeeperCPService("localhost:"+zkPort)

}

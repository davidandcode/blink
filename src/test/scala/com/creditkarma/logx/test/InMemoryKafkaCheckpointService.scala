package com.creditkarma.logx.test

import com.creditkarma.logx.base.CheckpointService
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint

class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
  var lastCheckPoint: KafkaCheckpoint = new KafkaCheckpoint()
  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
    lastCheckPoint = cp
  }
  override def lastCheckpoint(): KafkaCheckpoint = {
    lastCheckPoint
  }
}
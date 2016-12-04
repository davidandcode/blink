package com.creditkarma.logx.test

import com.creditkarma.logx.base.CheckpointService
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint

class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
  var lastCheckPoint: Option[KafkaCheckpoint] = None
  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
    lastCheckPoint = Option(cp)
  }
  override def lastCheckpoint(): Option[KafkaCheckpoint] = {
    lastCheckPoint
  }
}
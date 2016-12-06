package com.creditkarma.blink.test

import com.creditkarma.blink.base.CheckpointService
import com.creditkarma.blink.impl.checkpoint.KafkaCheckpoint

class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
  var lastCheckPoint: Option[KafkaCheckpoint] = None
  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
    lastCheckPoint = Option(cp)
  }
  override def lastCheckpoint(): Option[KafkaCheckpoint] = {
    lastCheckPoint
  }
}
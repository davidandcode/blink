package com.creditkarma.blink.test

import com.creditkarma.blink.base.StateTracker
import com.creditkarma.blink.impl.spark.tracker.kafka.KafkaCheckpoint

class InMemoryKafkaStateTracker extends StateTracker[KafkaCheckpoint]{
  var lastCheckPoint: Option[KafkaCheckpoint] = None
  override def persist(cp: KafkaCheckpoint): Unit = {
    lastCheckPoint = Option(cp)
  }
  override def lastCheckpoint(): Option[KafkaCheckpoint] = {
    lastCheckPoint
  }
}
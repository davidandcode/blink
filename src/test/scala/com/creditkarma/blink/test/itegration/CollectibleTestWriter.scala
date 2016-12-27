package com.creditkarma.blink.test.itegration

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId}

/**
  * This is for testing purpose, to collect what the single threaded writer have
  * We need to verify in the testing environment that the outputs have exactly the same information as the kafka source
  */
trait CollectibleTestWriter[Key, Value, Partition] {
  /**
    * This is the writer being tested
    * @return
    */
  def writer: ExportWorker[Key, Value, Partition]

  /**
    * This must be implemented based on knowledge of writer to collect information back
    * @return
    */
  def collect: Map[String, Seq[KafkaMessageWithId[Key, Value]]]

  def clearAll()
}

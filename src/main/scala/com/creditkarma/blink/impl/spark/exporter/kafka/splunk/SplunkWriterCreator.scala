package com.creditkarma.blink.impl.spark.exporter.kafka.splunk

import com.creditkarma.blink.factory.KafkaExportWorkerCreator

/**
  * Created by shengwei.wang on 12/20/16.
  */
class SplunkWriterCreator extends KafkaExportWorkerCreator[String, String, String]{

  override def writer: KafkaPartitionSplunkWriter = {

    val index = getOrFail("index")
    val indexers = getOrFail("indexers").split(";")
    val user = getOrFail("user")
    val password = getOrFail("password")
    val port = getOrFail("port").toInt

    new KafkaPartitionSplunkWriter(
      index,
      indexers,
      user,
      password,
      port
    )

  }

}

package com.creditkarma.blink.impl.spark.exporter.kafka.splunk

import com.creditkarma.blink.factory.KafkaExportWorkerCreator

/**
  * Created by shengwei.wang on 12/20/16.
  */
class SplunkTCPWriterCreator extends KafkaExportWorkerCreator[String, String, String]{

  override def writer: KafkaPartitionSplunkTCPWriter = {


    val indexers = getOrFail("indexers").split(";")
    val addresses = new Array[(String,String)](indexers.length)
    var i = 0
for(entry <- indexers){
addresses(i) = (entry.split(":")(0), entry.split(":")(1))
  i += 1
}

    new KafkaPartitionSplunkTCPWriter(
      addresses
    )

  }

}

package com.creditkarma.blink.impl.spark.exporter.kafka.local

/**
  * Created by shengwei.wang on 12/21/16.
  */




import com.creditkarma.blink.factory.KafkaExportWorkerCreator

/**
  * Created by shengwei.wang on 12/20/16.
  */
class LocalWriterCreator  extends KafkaExportWorkerCreator[String, String, String]{

  override def writer: LocalWriter = {

    val localPath = getOrFail("localPath")
    val localFileName = getOrFail("localFileName")

    new LocalWriter(
      localPath,
      localFileName
    )

  }

}
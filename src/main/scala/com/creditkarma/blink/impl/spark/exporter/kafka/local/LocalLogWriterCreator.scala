package com.creditkarma.blink.impl.spark.exporter.kafka.local

import com.creditkarma.blink.factory.KafkaExportWorkerCreator

class LocalLogWriterCreator extends KafkaExportWorkerCreator[String, String, String]{

  override def writer: LocalLogWriter = {

  val localFileName = getOrFail("localFileName")
  val maxFileSize = getOrFail("maxFileSize")
  val MaxBackupIndex = getOrFail("MaxBackupIndex")

  new LocalLogWriter(
  localFileName,
  maxFileSize,
  MaxBackupIndex
  )

}


}

package com.creditkarma.blink.impl.spark.exporter.kafka.local

import java.io.{File, FileWriter}

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.google.common.base.Throwables

import scala.util.{Failure, Success, Try}

/**
  * Created by shengwei.wang on 12/21/16.
  */
class LocalWriter  ( localPath:String, localFileName:String) extends ExportWorker[String, String, String] {
  override def useSubPartition: Boolean = false

  override def getSubPartition(payload: String): String = ""


  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {

   var lines =0
   var bytes=0

    Try({
      val directory: File = new File(localPath)
      if (!directory.exists()) {
        new File(localPath).mkdirs()
      }
      val fullName = s"${localPath}/${localFileName}"
      val pw = new FileWriter(new File(fullName),true)
      for (line <- data) {
        pw.write(line.value + System.lineSeparator())
        lines += 1
        bytes += line.value.getBytes().length
      }
      pw.close
    }) match {
      case Success(_) => new WorkerMeta(lines, bytes, true)
      case Failure(f) => new WorkerMeta(lines, bytes, false, Throwables.getStackTraceAsString(f))
    }


  }


}

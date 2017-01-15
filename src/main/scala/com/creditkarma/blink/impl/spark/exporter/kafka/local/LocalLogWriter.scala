package com.creditkarma.blink.impl.spark.exporter.kafka.local

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.google.common.base.Throwables
import org.apache.log4j._

import scala.util.{Failure, Success, Try}

/**
  * Created by shengwei.wang on 12/21/16.
  */
class LocalLogWriter(localFileName:String, maxFileSize:String, MaxBackupIndex:String) extends ExportWorker[String, String, String] {
  override def useSubPartition: Boolean = false
  override def getSubPartition(payload: String): String = ""

  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {

    var lines =0
    var bytes=0
    val logger = LocalLogWriter.getOrCreateMlogger(localFileName, maxFileSize, MaxBackupIndex)

    Try({
      for (line <- data) {
        logger.info(line.value)
        lines += 1
        bytes += line.value.getBytes().length
      }
    }) match {
      case Success(_) => new WorkerMeta(lines, bytes, true)
      case Failure(f) => new WorkerMeta(lines, bytes, false, Throwables.getStackTraceAsString(f))
    }
  }
}

object LocalLogWriter{
  var _mlogger: Option[Logger] = None
  def getOrCreateMlogger(fName:String, maxFileSize:String, MaxBackupIndex:String): Logger = _mlogger.synchronized {
    _mlogger.getOrElse {

      val mlogger = LogManager.getLogger(this.getClass)
      val mAppender = new RollingFileAppender()
      mAppender.setMaxFileSize(maxFileSize)
      mAppender.setMaxBackupIndex(MaxBackupIndex.toInt)
      mAppender.setFile(fName)
      mAppender.setLayout(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN));
      mAppender.activateOptions();
      mlogger.addAppender(mAppender)

      _mlogger = Some(mlogger)
      mlogger
    }
  }
}

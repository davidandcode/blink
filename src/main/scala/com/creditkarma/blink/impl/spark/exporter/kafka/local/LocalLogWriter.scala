package com.creditkarma.blink.impl.spark.exporter.kafka.local


import java.util.Properties

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.google.common.base.Throwables
import org.apache.log4j.{LogManager, Logger, PropertyConfigurator}

import scala.util.{Failure, Success, Try}

/**
  * Created by shengwei.wang on 12/21/16.
  */
class LocalLogWriter(localFileName:String,maxFileSize:String,MaxBackupIndex:String) extends ExportWorker[String, String, String] {
  override def useSubPartition: Boolean = false
  override def getSubPartition(payload: String): String = ""

  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {

    var lines =0
    var bytes=0
    val logger = LocalLogWriter.getOrCreateMlogger(localFileName,maxFileSize,MaxBackupIndex)

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
  def getOrCreateMlogger(fName:String,maxFileSize:String,MaxBackupIndex:String): Logger = _mlogger.synchronized {
    _mlogger.getOrElse {

      val props:Properties = new Properties()
      props.put("log4j.rootLogger", "INFO, FOO")
      //props.put("log4j.logger.com.creditkarma.blink.impl.spark.exporter.kafka.local.LocalLogWriter", "INFO, FOO")
      //props.put("log4j.logger.LocalLogWriter", "INFO, FOO")
      //props.put("log4j.logger.com.creditkarma.blink.impl.spark.exporter.kafka.local.LocalLogWriter", "true")
      //props.put("log4j.additivity.LocalLogWriter", "true")
      props.put("log4j.appender.FOO", "org.apache.log4j.RollingFileAppender")
      props.put("log4j.appender.FOO.layout", "org.apache.log4j.PatternLayout")
      props.put("log4j.appender.FOO.layout.ConversionPattern", "%m%n")
      props.put("log4j.appender.FOO.MaxFileSize", s"${maxFileSize}")
      props.put("log4j.appender.FOO.MaxBackupIndex", s"${MaxBackupIndex}")
      props.put("log4j.appender.FOO.File", s"${fName}")
      LogManager.resetConfiguration()
      PropertyConfigurator.configure(props)

      val mlogger = LogManager.getLogger(this.getClass)
      _mlogger = Some(mlogger)
      mlogger
    }
  }
}
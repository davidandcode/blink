package com.creditkarma.blink.impl.spark.exporter.kafka.splunk

import java.io._
import java.net.Socket

import com.creditkarma.blink.impl.spark.exporter.kafka.{ExportWorker, KafkaMessageWithId, SubPartition, WorkerMeta}
import com.google.common.base.Throwables
import com.splunk.{SSLSecurityProtocol, Service, _}

import scala.util.{Failure, Success, Try}

/**
  * Created by shengwei.wang on 12/10/16.
  */
class KafkaPartitionSplunkTCPWriter(address:Array[(String,String)]) extends ExportWorker[String, String, String]{

  override def useSubPartition: Boolean = false
  override def getSubPartition(payload: String): String = ""

  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {

    var i = 0
    var host = address(i)._1
    var port = address(i)._2

    var clientSocket:Socket = null

    // Create a Service instance and log in with the argument map
    while(clientSocket == null){
      try {
        clientSocket = new Socket(host, port.toInt)
      } catch{
        case e:Exception => {
          i += 1
          host = address(i)._1
          port = address(i)._2
        }
      }
    }

    var lines = 0
    var bytes = 0

Try({
      val ostream: OutputStream = clientSocket.getOutputStream
      val out: Writer = new OutputStreamWriter(ostream, "UTF8")
      // Send events to the socket then close it
      for(message <- data){
        out.write(message.value + "\r\n")
        lines += 1
        bytes += message.value.getBytes().length
      }
      out.flush()
    }) match {
      case Success(_) => {
        clientSocket.close()
        new WorkerMeta(lines, bytes, true)
      }
      case Failure(f) =>  {
        clientSocket.close()
        new WorkerMeta(lines, bytes, false, Throwables.getStackTraceAsString(f))
      }
    }
  }


}

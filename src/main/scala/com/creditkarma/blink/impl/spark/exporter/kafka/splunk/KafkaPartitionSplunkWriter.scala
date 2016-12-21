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
class KafkaPartitionSplunkWriter(index:String,indexers:Array[String],user:String,password:String,port:Int) extends ExportWorker[String, String, String]{

  override def useSubPartition: Boolean = false

  override def getSubPartition(payload: String): String = ""

  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {
    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2)
    var service: Service = null

    // Create a map of arguments and add login parameters
    val loginArgs: ServiceArgs = new ServiceArgs
    loginArgs.setUsername(user)
    loginArgs.setPassword(password)
    loginArgs.setPort(port)
    var i = 0
    loginArgs.setHost(indexers(i))

    // Create a Service instance and log in with the argument map
    while(service == null){
      try {
        service = Service.connect(loginArgs)
      } catch{
        case e:Exception => {
          i += 1
          loginArgs.setHost(indexers(i % indexers.length))
        }
      }
    }

    val myIndex: Index = service.getIndexes.get(index)
    val socket: Socket = myIndex.attach
    var lines = 0
    var bytes = 0

Try({
      val ostream: OutputStream = socket.getOutputStream
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
        socket.close()
        new WorkerMeta(lines, bytes, true)
      }
      case Failure(f) =>  {
        socket.close()
        new WorkerMeta(lines, bytes, false, Throwables.getStackTraceAsString(f))
      }
    }
  }


}

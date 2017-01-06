package com.creditkarma.blink.impl.spark.exporter.kafka.gcs

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.text.SimpleDateFormat
import java.util.Date

import com.creditkarma.blink.impl.spark.exporter.kafka._
import com.creditkarma.blink.utils.gcs.{GCSUtils, GzipCompressingInputStream}
import com.creditkarma.blink.utils.writer.CkAutoTsMessageParser
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject
import com.google.common.base.Throwables

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * All the constructor parameters needs to be configured upfront
  * The default values are controlled by the [[GCSWriterCreator]]
  *
  * @param tsName
  * @param partitionFormat
  * @param ifWithMicro
  * @param enforcedFields
  * @param credentialsPath
  * @param connectTimeoutMs
  * @param readTimeoutMs
  * @param bucketName
  * @param outputAppString
  * @param metaData
  * @param cacheControl
  * @param outputFileExtension
  * @param pathPrefix
  */
class GCSWriter(
      tsName: String,
      partitionFormat: String,
      ifWithMicro: Boolean,
      enforcedFields: String,
      credentialsPath: String,
      connectTimeoutMs: Int,
      readTimeoutMs: Int,
      bucketName: String,
      outputAppString: String,
      metaData: String,
      cacheControl: String,
      outputFileExtension: String,
      pathPrefix: String,
      compression:String) extends ExportWorker[String, String, String] {

  override def useSubPartition: Boolean = true

  /**
    * It's very important this method should never throw out exception
    * @param payload
    * @return
    */
  override def getSubPartition(payload: String): String = {
    // parser looks like a heavy object, instead of recreating a parser for each payload
    // we want to share a single parser object for the entire JVM
    val tsParser = GCSWriter.getOrCreateTsParser(tsName, ifWithMicro, enforcedFields)
    val timePartition = GCSWriter.getOrCreateTimePartitioner(partitionFormat)

    Try(tsParser.extractTimestampMillis(payload, ""))
    match {
      case Success(ts) => timePartition.timePartitionPath(ts)
      case Failure(f) => timePartition.timePartitionPath(0L)
    }
  }

  private def makeOutputPath(partition: SubPartition[String]): String = {
    s"${pathPrefix}/${partition.osr.topic}/${partition.subPartition.getOrElse("")}" +
      s"/${partition.osr.partition}_${partition.osr.fromOffset}.${outputFileExtension}"
  }

  // these are configuration time meta data like" priority,high;period,60
  private def metaDataKeyValue: Seq[(String, String)] = {
    for (keyString <- metaData.split(";"))
      yield {
        val keyValueSplits: Array[String] = keyString.split(",")
        Try(keyValueSplits(0) -> keyValueSplits(1))
        match {
          case Success((key, value)) => key -> value
          case Failure(f) => keyString -> "invalid meta"
        }
      }
  }

  /**
    * There are some Spark optimizations can be done here,
    * such as sharing GCS service object (it's thread safe), and sharing pre-configured meta data parsing and validation
    * @param partition sub-partition within the kafka topicPartition, such as time based partition
    * @param data stream of data to be written into a single atomic partition
    * @return meta data of writer client, the framework only requires number of records, total bytes and whether the write is 100% complete
    */
  override def write(partition: SubPartition[String], data: Iterator[KafkaMessageWithId[String, String]]): WorkerMeta = {
    var lines = 0L
    var bytes = 0L

    // guard against null payload for more general situations.
    // instead of simply filtering out null payload, we want to keep them so potential problems are exposed.
    def validPayload(message: KafkaMessageWithId[String, String]): String = Option(message.value).getOrElse("null")
    def payloadItr: Iterator[String] =
      data.map(validPayload).map{
        payload: String => {
          lines += 1 // update record count during the single pass triggered by gcs write request
          bytes += payload.size // update bytes during the single pass triggered by gcs write request
          payload + "\n" // return the JSON line
        }
      }
    // convert the iterator of payload strings into an InputStream, lazily evaluated
    def mStream: InputStream = new SequenceInputStream(
      payloadItr.map {
        payload => new ByteArrayInputStream(payload.getBytes("UTF-8"))
      }.asJavaEnumeration
    )

    def insertRequest = {
      val request =
        GCSUtils
          .getService(credentialsPath, connectTimeoutMs, readTimeoutMs)
          .objects()
          .insert(
            bucketName,
            new StorageObject()
              .setMetadata(metaDataKeyValue.toMap.asJava)
              .setCacheControl(cacheControl)
              .setName(makeOutputPath(partition)),
            compression.toLowerCase match {
              case "false" => new InputStreamContent (outputAppString, mStream)
              case "true" => new InputStreamContent (outputAppString, new GzipCompressingInputStream (mStream) )
            }
            )
      request.getMediaHttpUploader.setDirectUploadEnabled(true)
      request
    }

    Try(insertRequest.execute)
    match {
      case Success(_) => new WorkerMeta(lines, bytes, true)
      case Failure(f) => new WorkerMeta(lines, bytes, false, Throwables.getStackTraceAsString(f))
    }
  }
}

class TimePartitioner(format: String){
  private val dateFormat:SimpleDateFormat = new SimpleDateFormat(format)
  def timePartitionPath(ts: Long): String = dateFormat.format(new Date(ts))
}

/**
  * There is an alternative to use Spark broadcast variable, and lazy val to only block on initialization and speed up concurrent read-only access.
  * The following optimization is easier to implement and serves as a temporary solution.
  * This is the singleton object storing JVM level shared objects across task threads.
  * It is wasteful to re-create certain heavy objects for each data row, or even for each task.
  * Since these objects are not serializable with Spark tasks, and their arguments has to be serializable with the writer,
  * we can create these objects using singleton pattern.
  * For simplicity, we are assuming each object under this specific [[GCSWriterCreator]], is always created with the same set of parameters.
  * It's important that these objects must be thread safe. Waiting on the lock should be cheaper than recreating the object.
  * These following 2 objects are used in the [[GCSWriter.getSubPartition()]] method, called for every payload, and can be costly.
  * [[GCSWriter.write()]] method is at task level, and has less concern.
  */
object GCSWriter {
  var _tsParser: Option[CkAutoTsMessageParser] = None
  def getOrCreateTsParser(tsName: String, ifWithMicro: Boolean, enforcedFields: String): CkAutoTsMessageParser = _tsParser.synchronized {
    _tsParser.getOrElse {
      val tsParser = new CkAutoTsMessageParser(tsName, ifWithMicro, enforcedFields)
      _tsParser = Some(tsParser)
      tsParser
    }
  }

  var _timePartitioner: Option[TimePartitioner] = None
  def getOrCreateTimePartitioner(format: String): TimePartitioner = _timePartitioner.synchronized {
    _timePartitioner.getOrElse{
      val timePartitioner = new TimePartitioner(format)
      _timePartitioner = Some(timePartitioner)
      timePartitioner
    }
  }
}

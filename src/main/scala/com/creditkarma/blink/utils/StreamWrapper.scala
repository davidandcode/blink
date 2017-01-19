package com.creditkarma.blink.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.ReflectionUtils

import scala.collection.JavaConverters._

/**
  * Created by yongjia.wang on 1/18/17.
  * Wrap iterator as InputStream to keep the laziness and memory-saving nature, and add compression functionality at the same time
  */
object StreamWrapper {
  def apply(inputStream: Iterator[Byte], codec: Class[_ <: CompressionCodec]): InputStream = {
    new SequenceInputStream(
      new CompressedInputStream(inputStream, codec).map {
        payload => new ByteArrayInputStream(payload)
      }.asJavaEnumeration
    )
  }

  def apply(inputStream: Iterator[String], codec: Option[Class[_ <: CompressionCodec]] = None, charsetName: String = "UTF-8"): InputStream =
    codec match {
      case Some(c) => apply(inputStream.flatMap(_.getBytes(charsetName)), c)
      case None => new SequenceInputStream(
        inputStream.map {
          payload => new ByteArrayInputStream(payload.getBytes(charsetName))
        }.asJavaEnumeration)
    }

  /**
    * Using hadoop's compression API, parameters like buffer size can be configured through [[JobConf]]
    * @param inputStream
    * @param codec
    * @param conf
    */
  class CompressedInputStream(inputStream: Iterator[Byte], codec: Class[_ <: CompressionCodec], conf: JobConf = new JobConf()) extends Iterator[Array[Byte]] {

    private val buffer = new ByteArrayOutputStream()
    private val compressorStream = ReflectionUtils.newInstance(codec, conf).createOutputStream(buffer)

    override def hasNext: Boolean = inputStream.hasNext

    override def next(): Array[Byte] = {
      while(inputStream.hasNext && buffer.size() == 0){
        compressorStream.write(inputStream.next())
      }
      if(!inputStream.hasNext){
        compressorStream.close()
      }
      val compressedByteArray = buffer.toByteArray
      buffer.reset()
      compressedByteArray
    }
  }
}

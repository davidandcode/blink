package com.creditkarma.blink.test.unit

import java.io.{ByteArrayOutputStream, FileInputStream, InputStream}
import java.util.zip.GZIPInputStream

import com.creditkarma.blink.utils.StreamWrapper
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.scalatest.WordSpec

/**
  * Created by yongjia.wang on 1/18/17.
  */
class StreamWrapperTest extends WordSpec {

  def sampleFile: String = "README.md"
  def byteIteratorStream: Iterator[Byte] = {
    IOUtils.toByteArray(new FileInputStream(sampleFile)).iterator
  }

  def compressedInputStream: InputStream = StreamWrapper(byteIteratorStream, classOf[GzipCodec])
  val out1 = new ByteArrayOutputStream()
  IOUtils.copyLarge(new FileInputStream(sampleFile), out1)
  "A compressed stream" must {

    "have fewer bytes" in {
      val out2 = new ByteArrayOutputStream()
      IOUtils.copyLarge(compressedInputStream, out2)
      assert(out2.size() < out1.size())
    }

    "have identical bytes after decompression" in {
      val out2 = new ByteArrayOutputStream()
      IOUtils.copyLarge(new GZIPInputStream(compressedInputStream), out2)
      assert(out1.toByteArray.toSeq == out2.toByteArray.toSeq)
    }
  }
}

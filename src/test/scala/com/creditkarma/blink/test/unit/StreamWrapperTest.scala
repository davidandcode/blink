package com.creditkarma.blink.test.unit

import java.io._
import java.util.zip.GZIPInputStream

import com.creditkarma.blink.utils.StreamWrapper
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.scalatest.WordSpec

/**
  * Created by yongjia.wang on 1/18/17.
  */
class StreamWrapperTest extends WordSpec {


  "A compressed stream" must {
    def sampleFile: String = "README.md"
    def byteIteratorStream: Iterator[Byte] = {
      IOUtils.toByteArray(new FileInputStream(sampleFile)).iterator
    }

    def compressedInputStream: InputStream = StreamWrapper(byteIteratorStream, classOf[GzipCodec])
    val out1 = new ByteArrayOutputStream()
    IOUtils.copyLarge(new FileInputStream(sampleFile), out1)

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


  "A string iterator stream" must {
    val sampleInput = Seq("aaa", "bbb", "ccc")

    "have identical bytes in plain stream" in {
      // must explicitly append new line separator to each of the elements
      def plainStream: InputStream = StreamWrapper(sampleInput.iterator.map(_ + "\n"), None, "UTF-8")
      val plainStreamReader = new BufferedReader(new InputStreamReader(plainStream))
      for(line <- sampleInput){
        val readLine = plainStreamReader.readLine()
        assert(readLine == line)
      }
    }

    "have identical bytes after compression/decompression" in {
      // must explicitly append new line separator to each of the elements
      def compressedInputStream: InputStream = StreamWrapper(sampleInput.iterator.map(_ + "\n"), Some(classOf[GzipCodec]), "UTF-8")
      val decompressedStreamReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(compressedInputStream)))
      for(line <- sampleInput){
        val readLine = decompressedStreamReader.readLine()
        assert(readLine == line)
      }
    }
  }


}

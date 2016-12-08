package com.creditkarma.blink.test

import com.creditkarma.blink.impl.writer.GCSWriter

/**
  * Created by shengwei.wang on 12/7/16.
  */
object GCSWriterUnitTests {


  def main(args:Array[String]): Unit ={



    val mGCSWriter: GCSWriter =  new GCSWriter(
                     "ts",
                     true,
                     "",
                     "",
                     0,
                     0,
                     "dataeng",
                     "",
                     "",
                     "",
                     ""

                   )


    mGCSWriter.getSubPartition()



  }



}

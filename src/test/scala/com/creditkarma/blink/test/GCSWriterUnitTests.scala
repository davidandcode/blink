package com.creditkarma.blink.test

import com.creditkarma.blink.impl.writer.{GCSSubPartition, GCSWriter}

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


    //val testString:String = "2015-01-07T14:22:35.863513-08:00" //"2015-03-00T00:00:00-0800"
    val testString:String = "{\"dwNumericId\":5446427592603205633,\"traceId\":\"3a47bfd7-f050-424a-8bb3-1e84a3ab995a\",\"policyHolderId\":73,\"accidentIdx\":1,\"yearMonth\":\"2015-03-00T00:00:00-0800\",\"incidentType\":\"MINOR\",\"tsEvent\":\"1453492695.320\",\"driverIdx\":1,\"source\":\"web014.be.prod.iad1.ckint.io\",\"schemaName\":\"Accident.json\",\"version\":\"3c4eb462888500d9f161b4d637123e72\",\"ts\":\"2015-01-07T14:22:35.863513-08:00\"}"

    val mGCSSubPartition:GCSSubPartition = mGCSWriter.getSubPartition(testString)

    println(mGCSSubPartition.getYear)
    println(mGCSSubPartition.getMonth)
    println(mGCSSubPartition.getDay)
    println(mGCSSubPartition.getHour)
    println(mGCSSubPartition.getMinute)
    println(mGCSSubPartition.getSecond)

    val testString2:String = ""

    val mGCSSubPartition2:GCSSubPartition = mGCSWriter.getSubPartition(testString2)

    println(mGCSSubPartition2.getYear)
    println(mGCSSubPartition2.getMonth)
    println(mGCSSubPartition2.getDay)
    println(mGCSSubPartition2.getHour)
    println(mGCSSubPartition2.getMinute)
    println(mGCSSubPartition2.getSecond)

  }



}

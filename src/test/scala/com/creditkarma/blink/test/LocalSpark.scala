package com.creditkarma.blink.test

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yongjia.wang on 11/30/16.
  */
trait LocalSpark {
  startLocalSpark()
  def startLocalSpark(): Unit = {
    SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local[2]")
      .set("spark.driver.host", "127.0.0.1")
      // set local host explicitly, the call through java.net.InetAddress.getLocalHost on laptop with VPN can be inconsistent
      // Also if it returns IPV6, Spark won't work with it
    )

    LogManager.getLogger("org.apache").setLevel(Level.WARN)
    LogManager.getLogger("kafka").setLevel(Level.WARN)
  }
  def sc: SparkContext = SparkContext.getOrCreate()
}

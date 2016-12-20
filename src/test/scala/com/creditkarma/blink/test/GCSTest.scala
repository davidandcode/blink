package com.creditkarma.blink.test

import java.net.InetAddress

/**
  * Created by yongjia.wang on 12/14/16.
  */
trait GCSTest {
  val testType = "unit"
  // To make multiple concurrent tests on GCS safe, we need to use unique prefix, and optionally cleanup afterwards
  lazy val gcsPrefix = s"blink/test/${InetAddress.getLocalHost.getHostName}/${System.currentTimeMillis()}/gcs/$testType"
  lazy val gcsTestPath = s"dataeng_test/$gcsPrefix"
}

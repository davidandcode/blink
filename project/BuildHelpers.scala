import sbt._

/**
  * Potential setup for fine grained control of dependencies
  */
object Dependencies {
  val sparkSQL = "org.apache.spark" % "spark-sql_2.11" % "2.0.1"
  val sparkStreaming = "org.apache.spark" % "spark-streaming_2.11" % "2.0.1"
  val sparkStreamingKafka = "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.1"
  val jsonParserLib = "net.minidev" % "json-smart" % "2.2.1"
  val gcs = "com.google.apis" % "google-api-services-storage" % "v1-rev92-1.22.0"
  val splunk = "com.splunk" % "splunk" % "1.6.0.0"

  val kafkaUnit = "info.batey.kafka" % "kafka-unit" % "0.6"
  // kafkaUnit requires this
  val joptsimple = "org.apache.geode" % "gemfire-joptsimple" % "1.0.0-incubating.M1"
  // gcs hadoop connector is only used in test with Spark.
  // In production, blink use the more efficient gcs client API directly
  val gcsHadoop = "com.google.cloud.bigdataoss" % "gcs-connector" % "1.5.5-hadoop2"

  val mainDependencies = Seq(
    sparkSQL,
    sparkStreaming,
    sparkStreamingKafka,
    jsonParserLib,
    gcs,
    splunk)

  val testDependencies = Seq(kafkaUnit, joptsimple, gcsHadoop)

  val blink = mainDependencies ++ testDependencies.map(_ % Test)
}

object TestConfig {
  def isJenkinsBuildTest(test: String): Boolean = {
    test.endsWith("BlinkBasicIntegrationTest") || test.endsWith("KafkaTopicFilterTest")
  }
}

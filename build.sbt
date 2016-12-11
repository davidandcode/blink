name := "logx"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.1"

libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "1.5.4-hadoop2"

libraryDependencies += "info.batey.kafka" % "kafka-unit" % "0.6"

libraryDependencies += "org.apache.geode" % "gemfire-joptsimple" % "1.0.0-incubating.M1"

libraryDependencies += "net.minidev" % "json-smart" % "2.2.1"

testOptions in Test := Seq(Tests.Filter(s => s.endsWith("ZookeeperServiceTest")))








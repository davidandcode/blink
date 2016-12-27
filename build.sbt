lazy val commonSettings = Seq(
  name := "blink",
  version := "0.0.0",
  organization := "com.creditkarma",
  scalaVersion := "2.11.8",
  test in assembly := {}
)

lazy val blink = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("com.creditkarma.blink.MainApp"),
    resolvers += "splunk-artifactory" at "http://splunk.artifactoryonline.com/splunk/ext-releases-local",
    libraryDependencies ++= Dependencies.blink,

    assemblyMergeStrategy in assembly := {
      // discard all the meta inf
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    testOptions in Test := Seq(
      Tests.Filter(
        s => s.endsWith("BlinkBasicIntegrationTest") ||
          !s.contains("IntegrationTest") // only need to run 1 integration test, others should be covered by unit tests
      ))
    // more settings here ...
  )
import com.typesafe.sbt.packager.universal.UniversalPlugin

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
    libraryDependencies ++= Dependencies.blink,

    assemblyMergeStrategy in assembly := {
      // discard all the meta inf
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    testOptions in Test := Seq(
      Tests.Filter(
        s => TestConfig.isJenkinsBuildTest(s)
      ))
    // more settings here ...
  )

enablePlugins(UniversalPlugin)
def fileExistsOrFail(path: String): File = {
  val f = file(path)
  if(f.exists()) f else throw new Exception(s"File $path does not exist")
}
mappings in Universal in packageBin += fileExistsOrFail("config/kafka.gcs.properties") -> "conf/kafka.gcs.properties"
mappings in Universal in packageBin += fileExistsOrFail("config/log4j.properties") -> "conf/log4j.properties"
mappings in Universal in packageBin <+= (assembly in Compile) map { assemblyJar => assemblyJar -> s"bin/${assemblyJar.getName}" }

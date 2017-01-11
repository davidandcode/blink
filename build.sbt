import sbtassembly.AssemblyKeys

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

// define release task
TaskKey[File]("release") <<= (
  AssemblyKeys.assembly,
  Keys.target,
  Keys.name,
  Keys.version) map {
  (
    assemblyJar: File,
    target: File,
    name: String,
    version: String) =>

    val zipFile = target / (name + "-" + version + ".zip")
    val config = file("./config")

    // list directory recursively
    def entries(f: File): List[File] = f :: (if (f.isDirectory) IO.listFiles(f).toList.flatMap(entries(_)) else Nil)
    // zip configuration file and the assemblyJar
    IO.zip(entries(config).map(d => (d, d.getPath)) :+ (assemblyJar, assemblyJar.getName), zipFile)
    zipFile
}
name := "hdfs-ui"

organization := "com.github.lightcopy"

scalaVersion := "2.11.7"

// Compile dependencies
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" exclude("javax.servlet", "servlet-api") force()
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

javacOptions in ThisBuild ++= Seq("-Xlint:unchecked")
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "+q")

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}
// Exclude scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".dtd" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase startsWith "LICENSE_" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase startsWith "NOTICE_" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase startsWith "README_" => MergeStrategy.discard
  case other => MergeStrategy.first
}

coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}
coverageMinimum := 80
coverageFailOnMinimum := true

// Tasks dependencies
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile).dependsOn(compileScalastyle)

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value
(test in Test) <<= (test in Test).dependsOn(testScalastyle)

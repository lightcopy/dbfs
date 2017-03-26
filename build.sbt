name := "hdfs-ui"

organization := "com.github.lightcopy"

scalaVersion := "2.11.7"

// Compile dependencies
libraryDependencies ++= Seq(
  "org.glassfish.jersey.containers" % "jersey-container-servlet" % "2.25.1",
  "org.glassfish.jersey.containers" % "jersey-container-grizzly2-http" % "2.25.1",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25"
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
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.first
  // Exclude all static content from dependencies
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.endsWith(".html") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.exists(_ == "assets") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.exists(_ == "webapps") => MergeStrategy.discard
  // Exclude any contribs, licences, notices, readme from dependencies
  case PathList("contribs", xs @ _*) => MergeStrategy.discard
  case PathList("license", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase.startsWith("LICENSE") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase.startsWith("NOTICE") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase.startsWith("README_") => MergeStrategy.discard
  case other =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(other)
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

// MediSpark - Multi-Disease Clinical Risk Prediction Engine
// Build configuration for Spark 3.5.1 + Scala 2.12.18

name := "medispark"
version := "1.0"
scalaVersion := "2.12.18"

// Spark dependencies - marked "provided" because Spark cluster already has them
// This keeps our JAR small — only our code goes in, not all of Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"       % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-mllib"     % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.1" % "provided"
)

// Build a fat JAR (assembly) that bundles our code
// We use sbt-assembly plugin for this
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

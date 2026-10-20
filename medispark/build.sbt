// ==============================================================================
// MediSpark — Multi-Disease Clinical Risk Prediction Engine
// Spark 3.5.1 | Scala 2.12.18 | Hadoop 3.2.1
// ==============================================================================

name := "medispark"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"       % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-mllib"     % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.1" % "provided"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

name := "csv-to-avro-spark"
version := "0.1.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-avro" % "3.5.0",
  "com.typesafe" % "config" % "1.4.3",
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

ThisBuild / fork := true

Test / fork := true
Test / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
Test / javaOptions += "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"

Compile / run / fork := true
Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
Compile / run / javaOptions += "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x => MergeStrategy.first
}
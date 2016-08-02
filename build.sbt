name := "spark-streaming-amqp"

organization := "org.spark-project"

scalaVersion in ThisBuild := "2.11.8"

version := "0.1.0"

unmanagedResourceDirectories in Compile += baseDirectory.value / "python"

// **** from the sbt-spark-package plugin ****

spName := "org.spark-project/dstream-amqp" // the name of your Spark Package

sparkVersion in ThisBuild := "2.0.0" // the Spark Version your package depends on

sparkComponents in ThisBuild := Seq("streaming") // creates a dependency on spark-streaming

val vertxProton = "3.2.0"

libraryDependencies ++= Seq(
  // 2.0.0-preview brings jackson-module-scala_2.11:2.5.3 but Vert.x Proton brings all jackson.core JARs 2.6.5
  // there is a conflict and a Spark Streaming exception. I'm going to exclude jackson.core JARS from Vert.x Proton
  // Waiting for Spark 2.0.0-rc1 which fixes that
  //"io.vertx" % "vertx-proton" % vertxProton excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core")),
  "io.vertx" % "vertx-proton" % vertxProton,
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.activemq" % "activemq-broker" % "5.13.3" % "test",
  "org.apache.activemq" % "activemq-amqp" % "5.13.3" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided" classifier "tests"
)

val root = project in file(".")

val examples = project in file("examples") dependsOn (root % "compile->compile") settings (
  libraryDependencies ++= Seq(
    // Explicitly declare them to run examples using run-main.
    "org.apache.spark" %% "spark-core" % sparkVersion.value,
    "org.apache.spark" %% "spark-streaming" % sparkVersion.value
  )
)

// avoid to include all Scala packages into the fatjar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// to skip the test during assembly,
test in assembly := {}
name := "dstream-amqp"

organization := "com.redhat"

version := "0.0.1"

val sparkVersion = "1.6.1"

val vertexProton = "3.2.0"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % sparkVersion,
"org.apache.spark" %% "spark-streaming" % sparkVersion,
"io.vertx" % "vertx-proton" % vertexProton
)

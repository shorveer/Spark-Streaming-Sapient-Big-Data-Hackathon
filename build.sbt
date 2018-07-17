name := "KafkaStreaming"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0" 


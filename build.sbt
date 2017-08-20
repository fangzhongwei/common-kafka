name := "common-kafka"

version := "1.1"

scalaVersion := "2.12.2"

organization := "com.jxjxgo.common"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "net.codingwell" % "scala-guice_2.12" % "4.1.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.0"

name := "etlframework"

version := "0.1"

scalaVersion := "2.11.12"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.3" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6-M4"
libraryDependencies += "log4j" % "log4j" % "1.2.14"

assemblyMergeStrategy in assembly := {
  case PathList("scala", xs @ _*) => MergeStrategy.discard
  case "scala/*" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

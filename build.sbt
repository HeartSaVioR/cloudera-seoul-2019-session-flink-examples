
name := "cloudera-seoul-2019-session-flink-examples"

version := "0.1"

scalaVersion := "2.12.10"

val flinkVersion = "1.9.1"
val flinkScope = "provided"

resolvers += Resolver.mavenLocal
resolvers ++= List("Local Maven Repository" at "file:///" + Path.userHome.absolutePath + "/.m2/repository")

// scallop is MIT licensed
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "org.apache.flink" % "flink-json" % flinkVersion % flinkScope
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % flinkScope
libraryDependencies += "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion % flinkScope
libraryDependencies += "org.apache.flink" %% "flink-table-planner" % flinkVersion % flinkScope
libraryDependencies += "org.apache.flink" %% "flink-runtime-web" % flinkVersion % flinkScope

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

target in assembly := file("build")

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}.jar"


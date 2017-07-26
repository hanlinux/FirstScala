import AssemblyKeys._ // put this at the top of the file

name := "1212-PDCA-Parser"

version := "1.0.0"

scalaVersion := "2.11.8"

jarName in assembly := { s"${name.value}-assembly-${version.value}.jar" }

mainClass in assembly := Some("com.wisecloud.wiselyzer.parse.driver.ImportDriver")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers ++= Seq (
    "Apache Maven2 (REPO1)" at "http://10.57.198.45:8081/nexus/content/repositories/Apache_Maven2_REPO1/",
    "Apache_Maven2 Repository" at "http://10.57.198.45:8081/nexus/content/repositories/Apache_Meven2/",
    "Maven 2 Proxy  Repository" at "http://10.57.198.45:8081/nexus/content/repositories/central/",
    "Wiselyzer Repository" at "http://10.57.198.45:8081/nexus/content/repositories/wiselyzer/",
    "Grails Repository" at "http://10.57.198.45:8081/nexus/content/repositories/GrailsRepo/",
    "Slick" at "http://10.57.198.45:8081/nexus/content/repositories/Slick/"
)

libraryDependencies ++= Seq (
    //"org.apache.hadoop" % "hadoop-client" % "2.8.0" % "provided",
    "org.apache.hadoop" % "hadoop-common" % "2.8.0" % "provided",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.8.0" % "provided",
    //"org.apache.hadoop" % "hadoop-annotations" % "2.8.0" % "provided",
    //"org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.0" % "provided",
    //"org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.8.0" % "provided",
    //"org.apache.hadoop" % "hadoop-yarn-common" % "2.8.0" % "provided",
    
    "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
    //"org.apache.spark" %% "spark-hive" % "2.1.1" % "provided",
    //"org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided",
    //"org.apache.spark" %% "spark-mllib" % "2.1.1" % "provided",
    
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.1",
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1",
    
    "com.microsoft.sqlserver" % "sqljdbc4" % "4.0",
    
    "ch.qos.logback" % "logback-classic" % "1.1.7",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
    "com.typesafe.slick" %% "slick-extensions" % "3.0.0",
    "com.typesafe.slick" %% "slick-codegen" % "3.0.0",
    
    "com.fasterxml.jackson.core" % "jackson-core" % "2.1.1",
    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.1.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.1",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.1.1",
    
    "com.google.guava" % "guava" % "16.0.1",
    "org.apache.logging.log4j" % "log4j" % "2.8.2"
)

mergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) =>
        (xs map {_.toLowerCase}) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
                MergeStrategy.discard
            case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
                MergeStrategy.discard
            case "plexus" :: xs =>
                MergeStrategy.discard
            case "services" :: xs =>
                MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
                MergeStrategy.filterDistinctLines
            case _ => MergeStrategy.discard
        }
    case x => MergeStrategy.first
}

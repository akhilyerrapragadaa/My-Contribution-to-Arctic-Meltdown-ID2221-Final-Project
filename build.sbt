
scalaVersion := "2.11.8"

libraryDependencies ++= Seq( 
  guice
  ,"org.joda" % "joda-convert" % "1.9.2"
  ,"org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
  , "org.apache.spark" %% "spark-core" % "2.2.0"
  , "org.apache.spark" %% "spark-sql" % "2.2.0"
  , "org.apache.spark" %% "spark-mllib" % "2.2.0"
  ,  "org.vegas-viz" %% "vegas-spark" % "0.3.11"
  , "org.apache.hadoop" % "hadoop-client" % "2.7.2"
  , "com.cloudera.sparkts" % "sparkts" % "0.4.0"
  , ("com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2").exclude("io.netty", "netty-handler")
  , ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler")
)

// Stuff
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
  , "com.google.guava" % "guava" % "19.0"
)

// The Play project itself
lazy val root = (project in file("."))
  .enablePlugins(Common, PlayScala)
  .settings(
    name := """Play-with-Spark""",
  )


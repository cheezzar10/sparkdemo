organization := "edu.onefactor"
name := "onboarding"
version := "1.0-SNAPSHOT"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

// additional resolvers can be defined here

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
	"com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1",
	"org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

// mainClass in (Compile, packageBin) := Some("edu.1factor.Transformers")
// or can be specified using spark-submit --class 

// for JMH benchmarks only
// scalaSource in Jmh := baseDirectory.value / "src/jmh/scala"
// lazy val root = Project(id = "root", file(".")).enablePlugins(JmhPlugin)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// scalacOptions ++= Seq("-Xprint:typer")

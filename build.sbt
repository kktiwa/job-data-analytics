ThisBuild / scalaVersion := "2.12.15"

lazy val core = (project in file("job-data-analytics"))
  .settings(
    assembly / assemblyJarName := "job-data-analytics.jar",
    ThisBuild / libraryDependencies := allDependencies
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val dependencies = new {
  val sparkVersion = "3.1.2"
  val scalaTestVersion = "3.2.10"
  val enumeratumVersion = "1.7.0"

  //core dependencies
  val enumeratum = "com.beachape" %% "enumeratum" % enumeratumVersion
  val spark = Seq(
    "org.apache.spark" %% "spark-core",
    "org.apache.spark" %% "spark-sql"
  ).map(_ % sparkVersion)


  //test dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
}

lazy val allDependencies = dependencies.spark ++ Seq(
  dependencies.enumeratum, dependencies.scalaTest
)
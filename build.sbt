import AssemblyKeys._

assemblySettings

name := "template-scala-parallel-complementarypurchase"

organization := "org.apache.predictionio"

libraryDependencies ++= Seq(
  "org.apache.predictionio"    %% "apache-predictionio-core"          % pioVersion.value % "provided",
  "org.apache.spark" %% "spark-core"    % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.3.0" % "provided")


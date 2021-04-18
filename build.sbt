name := "Assignments"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
    "com.novocode" % "junit-interface" % "0.11" % Test,
    ("org.apache.spark" %% "spark-sql" % "2.3.2")
)
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")

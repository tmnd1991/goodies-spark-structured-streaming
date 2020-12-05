import Settings.{ scalaDocOptionsVersion, scalacOptionsVersion }

inThisBuild(
  List(
    organization := Settings.organization,
    homepage := Some(url(s"https://github.com/tmnd1991/${Settings.name}")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "tmnd1991",
        "Antonio Murgia",
        "ing.murgia@icloud.com",
        url("http://github.com/tmnd1991")
      )
    )
  )
)

name := Settings.name
scalaVersion := Settings.scala
libraryDependencies += "org.apache.spark"   %% "spark-sql"                          % "2.4.7" % Provided
libraryDependencies += "com.github.tmnd1991" %% "testing-spark-structured-streaming" % "0.0.1" % Test
crossScalaVersions := Settings.crossScalaVersions
description := "Goodies for Spark Structured Streaming"
scalacOptions ++= scalacOptionsVersion(scalaVersion.value)
scalastyleFailOnWarning := true
scalacOptions.in(Compile, doc) ++= scalaDocOptionsVersion(scalaVersion.value)

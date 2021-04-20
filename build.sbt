val fs2Version = "3.0.0-M9"
lazy val Fs2 = Seq(
  "co.fs2" %% "fs2-core",
  "co.fs2" %% "fs2-io"
).map(_%fs2Version)
lazy val MUnit      = "org.scalameta" %% "munit" % "0.7.23" % Test
lazy val ApacheCommonCompress = "org.apache.commons" % "commons-compress" % "1.20"
val circeVersion = "0.14.0-M5"
lazy val Circe =Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-literal",
).map(_ % circeVersion)
lazy val Jawn =  "org.typelevel" %% "jawn-parser" % "1.0.0"
lazy val PureConfig = "com.github.pureconfig" %% "pureconfig" % "0.14.1"

val http4sVersion = "1.0.0-M20"
lazy val Http4s = Seq(
  "org.http4s" %% "http4s-dsl" ,
  "org.http4s" %% "http4s-blaze-server" ,
  "org.http4s" %% "http4s-blaze-client" ,
  "org.http4s" %% "http4s-circe"
).map(_% http4sVersion)
lazy val Logback    = "ch.qos.logback" % "logback-classic" % "1.2.3"
lazy val app = (project in file(".")).settings(
  name := "cinvestav-ds-compressing",
  version := "0.1",
  scalaVersion := "2.13.5",
  libraryDependencies ++= Seq(MUnit,Jawn,ApacheCommonCompress,PureConfig,Logback) ++ Fs2 ++ Circe ++ Http4s
)
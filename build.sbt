scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "justwrote" at "http://repo.justwrote.it/releases/"

libraryDependencies ++= {
  Seq(
    "org.json4s"          %% "json4s-jackson"       % "3.2.11", 
    "mysql"               % "mysql-connector-java"  % "5.1.12",
    "it.justwrote"        %% "scala-faker"          % "0.3",
    "org.scalaj"          %% "scalaj-http"          % "1.1.4"
  )
}

options(error = NULL)

# Set Scala version
Sys.setenv("SCALA_VERSION" = "2.11")
Sys.setenv("SCALA_VERSION_MINOR" = ".8")

# Set Spark version
Sys.setenv("SPARK_VERSION" = "2.0")
Sys.setenv("SPARK_VERSION_MINOR" = ".2")

# Set scalac home
scala_version = paste0(Sys.getenv("SCALA_VERSION"), Sys.getenv("SCALA_VERSION_MINOR"))
Sys.setenv("SCALAC_HOME" = paste0("/usr/local/scala/scala-", scala_version, "/bin/scalac"))

rm(scala_version)

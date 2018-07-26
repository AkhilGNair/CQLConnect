#!/usr/bin/env r
# Get scala and spark versions from environment

if (!interactive()) source("./.Rprofile")

spark_version = Sys.getenv("SPARK_VERSION")
scala_version = Sys.getenv("SCALA_VERSION")

scalac_home = Sys.getenv("SCALAC_HOME")

sparklyr::spark_compile(
  jar_name = sprintf(
    "%s-%s-%s.jar",
    sparklyr:::infer_active_package_name(),
    spark_version,
    scala_version
  ),
  scalac = scalac_home,
  spark_home = sparklyr::spark_home_dir(),
  jar_dep = "inst/java/scalacass_2.11-0.6.14.jar"
)

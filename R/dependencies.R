spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/sparkhello-%s-%s.jar", spark_version, scala_version),
        package = "sparkhello"
      )
    ),
    packages = c(
      # "com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3",
      # "datastax:spark-cassandra-connector:2.0.0-RC1-s_2.11"
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

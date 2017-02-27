spark_dependencies <- function(spark_version, scala_version, ...) {
  message(paste("spark version:", spark_version, "| scala_version:", scala_version))
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/CQLConnect-%s-%s.jar", spark_version, scala_version),
        package = "CQLConnect"
      ),
      system.file(
        sprintf("java/scalacass_%s-0.6.14.jar", scala_version),
        package = "CQLConnect"
      )
    ),
    packages = c(
      # https://mvnrepository.com/artifact/com.google.guava/guava
      'com.google.guava:guava:16.0.1',
      # https://mvnrepository.com/artifact/com.chuusai/shapeless_2.11
      'com.chuusai:shapeless_2.11:2.3.2',
      # https://dl.bintray.com/thurstonsand/maven/com/github/thurstonsand/scalacass_2.11/0.6.14/
      'com.github.thurstonsand:scalacass_2.11:0.6.14',
      # https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11
      'com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3'
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

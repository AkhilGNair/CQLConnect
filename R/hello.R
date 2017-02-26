# Hello, world! - From Spark and Scala
#
# This is an example package which compiles
# a hello function in scala and deploys it
# to spark using sparklyr.
#
# You can learn more about sparklyr at:
#
#   http://spark.rstudio.com/
#

#' @import sparklyr
#' @export
cql_connect <- function(sc, cluster, keyspace) {
  sparklyr::invoke_static(sc, "CQLScalaCass.CQL", "cql_session", cluster, keyspace)
}


#' @import sparklyr
#' @export
get_row <- function(sc, session, table) {
  sparklyr::invoke_static(sc, "CQLScalaCass.CQL", "get_row", session, table)
}

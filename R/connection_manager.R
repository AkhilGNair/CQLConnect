#' Simple Cassandra session manager
#'
#' @name connection_manager
#'
#' @param sc Spark context used for the scala context
#' @param str_cluster The target cluster to which to connect
#' @param cluster A pointer to a build cluster (java object)
#' @param keyspace The keyspace to which to connect
NULL

#' @export
cql_cluster_connect <- function(sc, str_cluster) {
  sparklyr::invoke_static(sc, "CQLConnect.Connection", "cql_cluster_connect", str_cluster)
}

#' @export
cql_session_connect <- function(sc, cluster, keyspace) {
  sparklyr::invoke_static(sc, "CQLConnect.Connection", "cql_session_connect", cluster, keyspace)
}

#' @export
cql_session_close <- function(sc, session) {
  sparklyr::invoke_static(sc, "CQLConnect.Connection", "cql_session_close", session)
  TRUE
}

#' @export
cql_cluster_close <- function(sc, cluster, keyspace) {
  sparklyr::invoke_static(sc, "CQLConnect.Connection", "cql_cluster_close", cluster)
  TRUE
}

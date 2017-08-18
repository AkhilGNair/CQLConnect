#' Dispatch a count to some Cassandra table
#'
#' @param session The Cassandra session we will dispatch the query in
#' @param table The table to which to connect
#'
#' @export
cql_count <- function(sc, session, table) {
  sparklyr::invoke_static(sc, "CQLConnect.Count", "get_count", session, table)
}

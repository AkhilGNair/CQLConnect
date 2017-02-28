# Previous Poll module

#' Using the cassandra java driver directly from scala
#' to use CQL to query the database

#' @import sparklyr
#' @export
cql_connect <- function(sc, cluster, keyspace) {
  sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "cql_session", cluster, keyspace)
}


#' @import sparklyr
#' @export
cql_construct_query <- function(sc, keyspace, table) {
  sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "construct_query", keyspace, table)
}


#' @import sparklyr
#' @export
cql_previous_poll <- function(sc, session, query, str_date, str_vhid, str_loop_id, str_time) {
  sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "get_row", session, query, str_date, str_vhid, str_loop_id, str_time)
}

#' @import sparklyr
#' @export
cql_date_to_time <- function(sc, date) {
  sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "date_to_time", date)
}

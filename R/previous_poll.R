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
#' @include utils.R
#' @export
cql_previous_poll <- function(sc, session, query, str_date, str_vhid, str_loop_id, str_time) {
  # Pretty specific
  timestamp = stringr::str_replace(str_time, "([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}+)", "\\1T\\2")
  row = sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "get_row", session, query, str_date, str_vhid, str_loop_id, timestamp)
  row[[2]] = date_to_time(sc, row[[2]])  # Comes back with a java object to be converted to a timestamp
  row
}


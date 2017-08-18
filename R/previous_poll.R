#' Previous Poll module
#'
#' Build a CQL query for a precompiled model
#' Pull the retuned objects back into R
#'
#' @rdname PreviousPoll
#'
#' @param sc Spark context used for the scala context
NULL

#' Build the previous poll query
#' @param keyspace The keyspace to which to connect
#' @param table The table to which to connect
#'
#' @export
cql_previous_poll_query <- function(sc, keyspace, table) {
  sparklyr::invoke_static(sc, "CQLConnect.Queries", "previous_poll_query", keyspace, table)
}

#' Extract the previous poll R objects
#' @param session The cassandra session
#' @param query The prepared string to submit as CQL
#' @param str_date Date passed as str
#' @param str_vhid Vehicle ID passed as str
#' @param str_loop_id Loop id passed as str
#' @param str_time R timestamp to be converted to and interpreted by java
#'
#' @include utils.R
#' @export
cql_previous_poll <- function(sc, session, query, str_date, str_vhid, str_loop_id, str_time) {
  # Pretty specific
  timestamp = stringr::str_replace(str_time, "([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2})", "\\1T\\2")
  row = sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "get_poll_row", session, query, str_date, str_vhid, str_loop_id, timestamp)

  # Handle no returned row - ad hoc place holder
  if(length(row) < 2) { l = as.list(rep(NA_integer_, 13)); l[[2]] = as.POSIXct(NA_real_, origin = "1970-01-01"); return(l) }

  # A single row is returned as an R list
  row[[2]] = date_to_time(sc, row[[2]])  # The 2nd element (the date) comes back as a java object
  row                                    # A utility function post processes this back to R
}

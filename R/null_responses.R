#' Previous Poll module
#'
#' Build a CQL query for a precompiled model
#' Pull the retuned objects back into R
#'
#' @rdname NullResponses
#'
#' @param sc Spark context used for the scala context
#' @param session The keyspace to which to connect
#' @param date Date string, partition key in mv_polls
#' @param vhid Vehicle ID, partition key in mv_polls
#' @include utils.R
NULL

#' Get the partition of ordered telegrams for a vehicle
#'
#' @export
cql_get_null_response_partition <- function(sc, session, line, date, vhid) {
  sparklyr::invoke_static(sc, "CQLConnect.NullResponse", "get_df", session, as.integer(line), date, vhid)
}

#' Get the non-acknowledged messages
#'
#' @export
cql_get_non_ack_polls <- function(sc, iterator) {

  sparklyr::invoke_static(sc, "CQLConnect.NullResponse", "get_null_acknowledged_messages", iterator)

}

#' Get the non-acknowledged messages
#'
#' @export
cql_write_non_ack_polls <- function(sc, session, iterator) {

  sparklyr::invoke_static(sc, "CQLConnect.NullResponse", "write_null_acknowledged_messages", session, iterator)

}

#' Gets all polls for a partition, statefully filters non-acknowledged polls and writes to Cassandra
#'
#' @export
cql_non_ack_polls = function(sc, session, line, date, vhid) {

  # Get all polls
  df = cql_get_null_response_partition(sc, session, as.integer(line), date, vhid)

  # Filter out only non-acknowledged polls
  df_non_ack = cql_get_non_ack_polls(sc, df)

  # Write to Cassandra
  cql_write_non_ack_polls(sc, session, df_non_ack)

}


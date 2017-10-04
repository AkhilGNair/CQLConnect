#' Cassandra Table
#'
#' Access scala handles to cassandra tables
#' Using the spark-cassandra-connector
#'
#' @rdname CassandraTable
#' @include
NULL

#' Get a Cassandra table
#'
#' @export
cql_get_table <- function(sc, keyspace, table, select_cols = list()) {

  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "get_table", sc$spark_context, keyspace, table, select_cols)

}

#' Test joinWithCassandraTable
#'
#' @export
cql_get_obc_model <- function(sc, keyspace, table, date, select_cols = list()) {

  cols_reserved = c("line", "vehicle_id_command", "date", "vcc", "channel")
  reserved_cols_used = cols_reserved %in% select_cols

  if(any(reserved_cols_used))
    stop("Partition keys not necessary to specify, keys used: ", paste0(cols_reserved[reserved_cols_used], collapse = ", "))

  if(length(select_cols) > 0)
    select_cols = c(as.list(cols_reserved), select_cols)

  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "get_obc_model", sc$spark_context, keyspace, table, date, select_cols)

}

#' Show schema
#'
#' @export
cql_show_schema <- function(sc, keyspace, table) {
  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "show_schema", sc$spark_context, keyspace, table)
}

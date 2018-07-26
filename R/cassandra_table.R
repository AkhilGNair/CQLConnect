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
cql_get_obc_model <- function(sc, keyspace, line, date, select_cols = list()) {

  cols_reserved = c("line", "vehicle_id_command", "date", "vcc", "channel")
  reserved_cols_used = cols_reserved %in% select_cols

  # However the date is passed in, the scala method expects a character
  date = as.character(date)
  line = as.integer(line)

  if(any(reserved_cols_used))
    stop("Partition keys not necessary to specify, keys used: ", paste0(cols_reserved[reserved_cols_used], collapse = ", "))

  if(length(select_cols) > 0)
    select_cols = c(as.list(cols_reserved), select_cols)

  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "get_obc_model", sc$spark_context, keyspace, line, date, select_cols)

}

#' Get a Cassandra table
#'
#' @export
cql_get_schema <- function(sc, keyspace, table, select_cols = list()) {

  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "get_schema", sc$spark_context, keyspace, table)

}

#' Test joinWithRTable
#'
#' @export
cql_joinWithRTable <- function(sc, tbl_partitions, keyspace, table, select_cols = list()) {

  dataset = sparklyr::spark_dataframe(tbl_partitions)

  writer = sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "getWriter")
  reader = sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "getReader")

  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "joinWithRTable", 
    sc$spark_context, dataset, keyspace, table, select_cols, writer, reader)

}

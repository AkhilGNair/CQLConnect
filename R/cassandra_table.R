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
cql_get_table <- function(sc, keyspace, table) {
  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "get_table", sc$spark_context, keyspace, table)
}

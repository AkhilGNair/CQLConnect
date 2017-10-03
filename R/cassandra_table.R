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
cql_test_jwct <- function(sc, keyspace, table) {
  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "test_jwct", sc$spark_context, keyspace, table)
}

#' Show schema
#'
#' @export
cql_show_schema <- function(sc, keyspace, table) {
  sparklyr::invoke_static(sc, "CQLConnect.CassandraTable", "show_schema", sc$spark_context, keyspace, table)
}

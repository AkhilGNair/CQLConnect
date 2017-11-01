#' File Index module
#'
#' Build a CQL query for a precompiled model
#' Pull the retuned objects back into R
#'
#' @rdname DistMvFileIndex
#'
#' @param sc Spark context used for the scala context
#' @param session The cassandra session
NULL

#' Dispatch the Distinct File Index query
#'
#' @export
cql_dist_file_index_query <- function(sc, session) {
  sparklyr::invoke_static(sc, "CQLConnect.FileIndex", "query_dist_file_index", session)
}

#' Extract file_index row
#'
#' @include utils.R
#' @export
cql_get_file_index_row <- function(sc, session) {
  sparklyr::invoke_static(sc, "CQLConnect.FileIndex", "get_dist_file_index_row", session)
}

#' Exhaust file_index row iterator to get whole file_index
#'
#' @include utils.R
#' @export
cql_iterate <- function(sc, iterator) {
  sparklyr::invoke_static(sc, "CQLConnect.FileIndex", "get_dist_file_index", iterator)
}

#' Exhaust file_index row iterator to get whole file_index
#'
#' @include utils.R
#' @export
cql_hasnext <- function(sc, iterator) {
  sparklyr::invoke_static(sc, "CQLConnect.FileIndex", "iterator_exhausted", iterator)
}

#' Exhaust file_index row iterator to get whole file_index
#'
#' @import data.table
#' @include utils.R
#' @export
cql_get_file_index <- function(sc, session) {

  # Table count to preallocate memory for the iterator
  n = cql_count(sc, session, "mv_file_index")

  # Iterator of a table row
  iterator = cql_get_file_index_row(sc, session)

  # Exhaust the iterator through a map
  dtl = lapply(seq_along(1:n), function(i) {
    sparklyr::invoke_static(sc, "CQLConnect.FileIndex", "get_dist_file_index", iterator)
  })

  # Transpose the nested list and set as a data.table
  dt = split(unlist(dtl, use.names = FALSE), c("line", "analysed", "date", "file_name", "state"))
  data.table::setDT(dt)

  # Some casting which could be avoided if not for unlist
  dt[, analysed := as.logical(analysed)]
  dt[, line := as.integer(line)]
  dt[, date := as.Date(as.numeric(date), origin = "1970-01-01")]

  # Call [.data.table so function returns an object
  dt[]
}

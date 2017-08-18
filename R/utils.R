#' @import sparklyr
date_to_time <- function(sc, java_date) {
  sparklyr::invoke_static(sc, "CQLConnect.DateUtils", "date_to_time", java_date)
}

#' @import sparklyr
date_to_time <- function(sc, date) {
  sparklyr::invoke_static(sc, "CQLConnect.PreviousPoll", "date_to_time", date)
}

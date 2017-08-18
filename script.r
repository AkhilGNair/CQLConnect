options(error = NULL)

library(sparklyr)
library(dplyr)
library(CQLConnect)

sc <- spark_connect(master = "local", config = spark_config("~/work/cipher/cipher-parser/docker/conf/sparklyr/dev-spark-config.yml"))

session = cql_connect(sc, 'localhost', 'dev_cipher_jubilee')
query = cql_previous_poll_query(sc, 'dev_cipher_jubilee', 'mv_type0s')
cql_previous_poll(sc = sc, session = session, query = query, str_date = "2017-05-14", str_vhid = "4", str_loop_id = "10", str_time = "2017-05-14 02:02:41.660000")

CQLConnect::cql_previous_poll(
            sc,
            session = session,
            query = query,
            str_date = '2017-05-14',
            str_vhid = as.integer(1),
            str_loop_id = as.integer(1),
            str_time = '2017-05-14 17:02:42.080000'
)

env = Sys.getenv("CIPHER_ENV")
if (env == "") stop("No app environment set!")
if (env == "test") home = "~/work/cipher/cipher-coordinator"
if (env == "development") home = "~/work/cipher/cipher-coordinator"
if (env == "production") home = "/root/cipher/vendor/github.com/amey-sam/cipher-coordinator"

# Config object
config = config::get(file = file.path(home, "app-config.yml"), config = env)
str_keyspace = config$KEYSPACE


tbl_hc = crassy::spark_load_cassandra_table(
  sc = sc,
  cass_keyspace = 'dev_cipher_jubilee',
  cass_tbl = str_healthcodes_find_pp,
  spk_tbl_name = str_spk_healthcodes,
  partition_filter = str_where_clause
)

# Connect Scala to a session
session = cql_connect(sc, str_cluster, str_keyspace)
query = cql_construct_query(sc, str_keyspace, str_mv_type0s)

# Locally collect healthcodes to query scala
# TODO: Change to hc_actual_loop_id
local_tbl_hc = tbl_hc %>% select(date, channel, vcc, vehicle_id_command, current_loop_id, time) %>% collect()

n_rows = local_tbl_hc %>% summarise(count = n()) %>% `[[`("count")
if(n_rows == 0) { rm(local_tbl_hc); invisible(gc()); return("No healthcodes found") }

# Submit the query, grab the results in a last_poll list-column
local_tbl_hc = local_tbl_hc %>%
  rowwise() %>%
  mutate(last_poll = list(
    cql_previous_poll(
      sc,
      session = session,
      query = query,
      str_date = date,
      str_vhid = as.integer(vehicle_id_command),
      str_loop_id = as.integer(current_loop_id),
      str_time = time)
  ))

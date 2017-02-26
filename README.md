# SparklyrCassandraConnector
A packages to further connect Sparklyr to Cassandra via the Datastax spark-cassandra-connector

Initially forked from https://github.com/javierluraschi/sparkhello

```
library(sparklyr)
library(sparkhello)

# spark_compile(jar_name = sprintf("%s-2.0-2.11.jar", sparklyr:::infer_active_package_name()), 
#               scalac = "/usr/local/scala/scala-2.11.8/bin/scalac", 
#               spark_home = spark_home_dir())

sc = sparklyr::spark_connect(master = 'local', spark_home = sparklyr::spark_home_dir())

session = sparkhello:::cql_connect(sc, "localhost", "keyspace1")
row = sparkhello:::get_row(sc, session, "playlists")

ptm = proc.time()
for (i in 1:1000) { 
  sparkhello:::get_row(sc, session, "playlists") 
}
proc.time() - ptm
```

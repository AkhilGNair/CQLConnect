package CQLConnect

import java.sql
import java.util.Date
import scala.reflect.ClassTag

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD

import CQLConnect.DateUtils.DateFormatter

object CassandraTable {

  // Map Cassandra elements to Spark
  def convertToSpark(element:Any): Any = element match {
    case time: org.joda.time.LocalDate => new sql.Date(time.toDateTimeAtStartOfDay().getMillis) // Convert to java.sql.Date
    case date: java.util.Date => new sql.Timestamp(date.getTime)
    case uuid: java.util.UUID => uuid.toString()
    case other => other
  }

  // Get a Cassandra table schema
  def get_schema(sc: SparkContext, keyspace: String, table: String) = {
    val spark =  SparkSession.builder().getOrCreate()
    spark.read.cassandraFormat(table, keyspace).load.schema
  }

  def get_table(sc: SparkContext, keyspace: String, table: String, select_cols: Array[String]) = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._
    // Cassandra schema and table
    var schema = get_schema(sc, keyspace, table)
    var cass_table = sc.cassandraTable(keyspace, table)
    // Select columns is necessary
    if (select_cols.length > 0) {
      schema = StructType(schema.filter(x => select_cols.contains(x.name)))
      cass_table = cass_table.select(select_cols.map(ColumnName(_)):_*)
    }
    // Convert Cassandra values to Spark and return
    val spk_cass = cass_table.map{ case cassandraRow => Row(cassandraRow.columnValues.map(convertToSpark):_*) }
    spark.createDataFrame(spk_cass, schema)
  }

  // Hardcode function to pull obc_model
  //  - Read in partitions model from C*
  //  - Map into a table with an added date
  //  - Repartition by obc_model partitioner to distribute load correctly
  //  - Join with C8 table to return the subset RDD
  //  - Filter if necessary
  //  - Map C* values to Spark Dataset to hand to Sparklyr
  def get_obc_model ( sc: SparkContext, keyspace: String, int_line: Int, str_date: String, select_cols: Array[String]) = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._

    var date: sql.Date = new sql.Date(DateFormatter.parse(str_date).getTime())
    var schema = get_schema(sc, keyspace, "obc_model")

    // Repartitioning to the spark cluster as
    // partitions_obc_model is only distributed across 2 nodes
    var cass_join =
      sc.cassandraTable(keyspace, "partitions_obc_model")
        .where("line = ?", int_line)
        // TODO: Case class this
        .map{ case cassandraRow => (
          cassandraRow.getInt("line"),
          cassandraRow.getInt("vehicle_id_command"),
          date,
          cassandraRow.getInt("vcc"),
          cassandraRow.getInt("channel")
        )}
        .repartitionByCassandraReplica(keyspace, "obc_model")
        .joinWithCassandraTable(keyspace, "obc_model")

    if (select_cols.length > 0) {
      schema = StructType(schema.filter(x => select_cols.contains(x.name)))
      cass_join = cass_join.select(select_cols.map(ColumnName(_)):_*)
    }

    val spk_cass_join = cass_join.map{ case(_, cassandraRow) => Row(cassandraRow.columnValues.map(convertToSpark):_*)}
    spark.createDataFrame(spk_cass_join, schema)
  }

  // Can the partitions table be supplied via Sparklyr?
  def joinWithRTable[T <: Serializable: ClassTag] (
    sc: SparkContext,
    dataset: Dataset[T],
    keyspace: String,
    table: String,
    select_cols: Array[String])(
    implicit
      rwf: writer.RowWriterFactory[T],
      rrf: rdd.reader.RowReaderFactory[T]
    ) = {

    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._

    val my_rdd = dataset.rdd
    var schema = get_schema(sc, keyspace, table)
    var cass_join =
      my_rdd
        .repartitionByCassandraReplica(keyspace, table)
        .joinWithCassandraTable(keyspace, table)

    if (select_cols.length > 0) {
      schema = StructType(schema.filter(x => select_cols.contains(x.name)))
      cass_join = cass_join.select(select_cols.map(ColumnName(_)):_*)
    }

    cass_join
  }

}

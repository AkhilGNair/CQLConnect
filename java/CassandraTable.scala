package CQLConnect

import java.sql
import java.sql.Timestamp
import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import CQLConnect.DateUtils.DateFormatter

object CassandraTable {

  val spark =  SparkSession.builder().getOrCreate()
  import spark.implicits._

  def convertToSpark(element:Any): Any = element match {
    case time: org.joda.time.LocalDate => new sql.Date(time.toDateTimeAtStartOfDay().getMillis) // Convert to java.sql.Date
    case date: java.util.Date => new Timestamp(date.getTime)
    case uuid: java.util.UUID => uuid.toString()
    case other => other
  }


  def get_table ( sc: SparkContext, keyspace: String, table: String, select_cols: Array[String]) = {

    var schema = spark.read.cassandraFormat(table, keyspace).load.schema

    var cass_table = sc.cassandraTable(keyspace, table)

    if (select_cols.length > 0) {
      schema = StructType(schema.filter(x => select_cols.contains(x.name)))
      cass_table = cass_table.select(select_cols.map(ColumnName(_)):_*)
    }

    val spk_cass = cass_table.map{ case cassandraRow => Row(cassandraRow.columnValues.map(convertToSpark):_*) }

    val dataset = spark.createDataFrame(spk_cass, schema)

     dataset

  }

  def get_obc_model ( sc: SparkContext, keyspace: String, table: String, str_date: String, select_cols: Array[String]) = {

    var date: sql.Date = new sql.Date(DateFormatter.parse(str_date).getTime())

    var schema = spark.read.cassandraFormat(table, keyspace).load.schema

    var cass_join =
      sc.cassandraTable(keyspace, "partitions_obc_model")
        .map{ case cassandraRow => (
          cassandraRow.getInt("line"),
          cassandraRow.getInt("vehicle_id_command"),
          date,
          cassandraRow.getInt("vcc"),
          cassandraRow.getInt("channel")
        ) }
        .joinWithCassandraTable(keyspace, table)

    if (select_cols.length > 0) {
      schema = StructType(schema.filter(x => select_cols.contains(x.name)))
      cass_join = cass_join.select(select_cols.map(ColumnName(_)):_*)
    }

    val spk_cass_join = cass_join.map{ case(_, cassandraRow) => Row(cassandraRow.columnValues.map(convertToSpark):_*)}
    val dataset = spark.createDataFrame(spk_cass_join, schema)

    dataset

  }

}

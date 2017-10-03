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


object CassandraTable {

  val spark =  SparkSession.builder().getOrCreate()
  import spark.implicits._

  def convertToSpark(element:Any): Any = element match {
    case time: org.joda.time.LocalDate => new sql.Date(time.toDateTimeAtStartOfDay().getMillis) // Convert to java.sql.Date
    case date: java.util.Date => new Timestamp(date.getTime)
    case uuid: java.util.UUID => uuid.toString()
    case other => other
  }


  def get_table ( sc: SparkContext, keyspace: String, table: String, select: Array[String]) = {

    var schema = spark.read.cassandraFormat(table, keyspace).load.schema

    var cass_table = sc.cassandraTable(keyspace, table)

    if (select.length > 0) {
      schema = StructType(schema.filter(x => select.contains(x.name)))
      cass_table = cass_table.select(select.map(ColumnName(_)):_*)
    }

    val spk_cass = cass_table.map{ case cassandraRow => Row(cassandraRow.columnValues.map(convertToSpark):_*) }

    val dataset = spark.createDataFrame(spk_cass, schema)

     dataset

  }

  def test_jwct ( sc: SparkContext, keyspace: String, table: String ) = {

    val schema = spark.read.cassandraFormat(table, keyspace).load.schema

    val joinResult =
      sc.cassandraTable(keyspace, "partitions_obc_model").where("date = '2017-05-13'")
        .joinWithCassandraTable(keyspace, table)
        .map{ case(_, cassandraRow) => Row(cassandraRow.columnValues.map(convertToSpark):_*)}

    val dataset = spark.createDataFrame(joinResult, schema)

    dataset

  }

}

package CQLConnect

import java.sql
import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import CQLConnect.Models.DistMVFileIndexModel
import CQLConnect.DateUtils.convert_cld_to_d
import com.datastax.driver.core.LocalDate

object CassandraTable {

  val spark =  SparkSession.builder().getOrCreate()
  import spark.implicits._

  def convertToSpark(element:Any): Any = element match {
    case time: org.joda.time.LocalDate => new sql.Date(time.toDateTimeAtStartOfDay().getMillis) // Convert to java.sql.Date
    case other => other
  }

  def get_table ( sc: SparkContext, keyspace: String, table: String ) = {

    val schema = spark.read.cassandraFormat(table, keyspace).load.schema

    val joinResult =
      sc.cassandraTable(keyspace, table)
        .map{ case cassandraRow => Row(cassandraRow.columnValues.map(convertToSpark):_*)}

    val dataset = spark.createDataFrame(joinResult, schema)

     dataset

  }

}

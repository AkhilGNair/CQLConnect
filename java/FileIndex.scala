package CQLConnect

import java.util.Date
import scala.language.implicitConversions

import com.datastax.driver.core.{ Row, LocalDate }
import com.weather.scalacass._
import com.weather.scalacass.syntax._

import CQLConnect.Models.{ DistMVFileIndexModel, extract_dist_mvfileindex_values }


object FileIndex {

  def query_dist_file_index( session:ScalaSession ) : Iterator[Row] = {
    val keyspace = session.keyspace

    val query =
      s"""SELECT
         |  line,
         |  analysed,
         |  date,
         |  file_name,
         |  state
         |FROM $keyspace.mv_file_index""".stripMargin

    session.rawSelect(query)

  }

  def get_dist_file_index_row( session:ScalaSession ) : Iterator[Row] = {
    // Query cassandra to get the row, converting R values to Cassandra mappable classes
    query_dist_file_index(session)
  }

  def iterator_exhausted( df:Iterator[Row] ) : Boolean = {
    df.hasNext
  }

  def get_dist_file_index( df:Iterator[Row] ) : Array[Any] = {
    // If there is no row, return an empty array
    if(df.isEmpty) { return(Array():Array[Any]) }
    // If a row exists, puts the row into a model
    val row: Row = df.next()
    val model: DistMVFileIndexModel = row.as[DistMVFileIndexModel]
    // Return the extracted values from the model in a list
    extract_dist_mvfileindex_values(model)
  }

}

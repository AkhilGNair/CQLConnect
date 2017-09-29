package CQLConnect

import scala.language.implicitConversions

import CQLConnect.Models.{ PollModel, extract_poll_values }
import CQLConnect.DateUtils.DateFormatter

import java.util.Date
import org.joda.time.DateTime

import com.datastax.driver.core.{ Row, LocalDate }
import com.weather.scalacass._
import com.weather.scalacass.syntax._


object PreviousPoll {

  def query_poll_row( session:ScalaSession, query:String, int_line:Int, str_date:String, str_vhid:Int, str_loop_id:Int, str_time:String ) : Iterator[Row] = {
    val time:Date = DateTime.parse(str_time).toDate
    val date:Date = DateFormatter.parse(str_date)
    val localdate:LocalDate = LocalDate.fromMillisSinceEpoch(date.getTime())
    session.rawSelect(query, Int.box(int_line), localdate, Int.box(str_vhid), Int.box(str_loop_id), time)
  }

  def get_poll_row( session:ScalaSession, query:String, int_line:Int, str_date:String, int_vhid:Int, int_loop_id:Int, str_time:String ) : Array[Any] = {
    // Query cassandra to get the row, converting R values to Cassandra mappable classes
    val aRow: Iterator[Row] = query_poll_row(session, query, int_line, str_date, int_vhid, int_loop_id, str_time)  // LIMIT 1 query, select next
    // If there is no row, return an empty array
    if(aRow.isEmpty) { return(Array():Array[Any]) }
    // If a row exists, puts the row into a model
    val foundRow: Row = aRow.next()
    val model: PollModel = foundRow.as[PollModel]
    // Return the extracted values from the model in a list
    extract_poll_values(model)
  }

}

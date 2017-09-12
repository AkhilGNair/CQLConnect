package CQLConnect

import scala.language.implicitConversions

import CQLConnect.Models.{ NullResponseModel, extract_null_response_values }
import CQLConnect.DateUtils.DateFormatter
import CQLConnect.Queries.null_response_query

import java.util.Date
import org.joda.time.DateTime

import com.datastax.driver.core.{ Row, LocalDate }
import com.weather.scalacass._
import com.weather.scalacass.syntax._


object NullResponse {

  private def uninitialised_loop(loop: (Int, Int)): Boolean = loop == (0, 0)

  private def get_loop(row: NullResponseModel): (Int, Int) = (row.vcc, row.channel)

  private def get_command_response_type(row: NullResponseModel): String = row.command_response_type

  private def different_loop(row: NullResponseModel, current_loop: (Int, Int)): Boolean = {
    get_loop(row) != current_loop
  }

  private def same_loop(row: NullResponseModel, current_loop: (Int, Int)): Boolean = {
    get_loop(row) == current_loop
  }

  private def connected_to_next_loop(row: NullResponseModel, current_loop: (Int, Int)): Boolean = {
    different_loop(row, current_loop) & get_command_response_type(row) == "PollType0"
  }

  private def null_acknowledged_message(row:NullResponseModel, current_loop: (Int, Int)): Boolean = {
    same_loop(row, current_loop) & (get_command_response_type(row) == "PollNull")
  }

  def get_df( session:ScalaSession, str_date:String, str_vhid:Int ) : Iterator[Row] = {

    val query: String = null_response_query(session)

    val date:Date = DateFormatter.parse(str_date)
    val localdate:LocalDate = LocalDate.fromMillisSinceEpoch(date.getTime())

    session.rawSelect(query, localdate, Int.box(str_vhid))

  }

  def get_null_acknowledged_messages(df: Iterator[Row]): Iterator[NullResponseModel] = {

    var current_loop = (0, 0)
    var non_acknowledged_messages = Stream.empty[NullResponseModel]
    var connected = false

    if (!df.hasNext) return Iterator.empty

    while (!connected && df.hasNext) {

      var this_row = df.next().as[NullResponseModel] // df is a row iterator

      // Connect to first loop, update the current loop, break out
      if (connected_to_next_loop(this_row, current_loop)) {
        current_loop = get_loop(this_row)
        connected = true
      }

      if (!connected && row.command_response_type == "PollNull") non_acknowledged_messages :+= this_row

    }


    while (df.hasNext) {

      var this_row = df.next().as[NullResponseModel] // df is a row iterator

      if (null_acknowledged_message(this_row, current_loop)) {
        non_acknowledged_messages :+= this_row
      }

      if (connected_to_next_loop(this_row, current_loop)) current_loop = get_loop(this_row)

    }

    non_acknowledged_messages.toIterator

  }

  def write_null_acknowledged_messages(session:ScalaSession, df: Iterator[NullResponseModel]): Boolean = {

    // Hardcode in table name
    while(df.hasNext) session.insert("raw_non_ack", df.next())

    true

  }

}

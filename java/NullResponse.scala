package CQLConnect

import scala.language.implicitConversions

import CQLConnect.Models.NullResponseModel
import CQLConnect.DateUtils.DateFormatter
import CQLConnect.Queries.null_response_query

import java.util.Date
import org.joda.time.DateTime

import com.datastax.driver.core.{ Row, LocalDate }
import com.weather.scalacass._
import com.weather.scalacass.syntax._


object NullResponse {

  // Gets the loop ID - tuple off VCC number and channel
  def get_loop(row: NullResponseModel): (Int, Int) = (row.vcc, row.channel)

  // Gets the command response type of a message
  def get_command_response_type(row: NullResponseModel): String = row.command_response_type

  // A check to see if loop has changed since the previous message
  def different_loop(row: NullResponseModel, current_loop: (Int, Int)): Boolean = {
    get_loop(row) != current_loop
  }

  // If we have a PollType0 on a new loop, we connect to the new loop
  def connected_to_next_loop(row: NullResponseModel, current_loop: (Int, Int)): Boolean = {
    different_loop(row, current_loop) & get_command_response_type(row) == "PollType0"
  }

  // If we are on the same loop and miss a pool these are the messages we're
  // interested in
  def null_acknowledged_message(row:NullResponseModel, current_loop: (Int, Int)): Boolean = {
    !different_loop(row, current_loop) & (get_command_response_type(row) == "PollNull")
  }

  // Uses a prepared query to request the data for a (line, date, vehicle) bucket
  def get_df( session:ScalaSession, line: Int, str_date:String, str_vhid:Int ) : Iterator[Row] = {
    val query: String = null_response_query(session)
    val date:Date = DateFormatter.parse(str_date)
    val localdate:LocalDate = LocalDate.fromMillisSinceEpoch(date.getTime())
    session.rawSelect(query, Int.box(line), localdate, Int.box(str_vhid))
  }

  // Ticks through each message in the dataset and accululates those which fit the
  // null_acknowledged_message criteria
  def get_null_acknowledged_messages(df: Iterator[Row]): Iterator[NullResponseModel] = {
    var current_loop = (0, 0)
    var non_acknowledged_messages = List.empty[NullResponseModel]
    var connected = false
    if (!df.hasNext) return(Iterator.empty) // Return empty if no data for bucket

    // Track rows that we don't record until connect to the first loop
    while (!connected && df.hasNext) {
      var this_row = df.next().as[NullResponseModel]  // df.next() is a row
      // Connect to first loop, update the current loop, break out
      if (connected_to_next_loop(this_row, current_loop)) {
        current_loop = get_loop(this_row)
        connected = true
      }
      if (!connected && this_row.command_response_type == "PollNull") {
        non_acknowledged_messages :+= this_row
      }
    }

    // After we first connect, null messages are found by normal criteria
    // Push them into non_acknowledged_messages dataset
    while (df.hasNext) {
      var this_row = df.next().as[NullResponseModel]  // df.next() is a row
      if (null_acknowledged_message(this_row, current_loop)) non_acknowledged_messages :+= this_row
      if (connected_to_next_loop(this_row, current_loop)) current_loop = get_loop(this_row)
    }

    // Return dataset of non-acknowledges messages
    non_acknowledged_messages.toIterator
  }

  // Write all messages to C*
  def write_null_acknowledged_messages(session:ScalaSession, df: Iterator[NullResponseModel]): Boolean = {
    // Hardcode in table name
    while(df.hasNext) session.insert("raw_non_ack", df.next())
    true
  }

}

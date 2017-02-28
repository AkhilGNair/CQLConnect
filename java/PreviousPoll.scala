package CQLConnect

import scala.language.implicitConversions

import java.sql.Timestamp
import java.util.Date
import java.text.SimpleDateFormat
import org.joda.time.DateTime

import com.datastax.driver.core.{ Cluster, Row, LocalDate }
import com.weather.scalacass._
import com.weather.scalacass.syntax._

object PreviousPoll {

  def construct_query( keyspace: String , table: String ) : String = {
      val selectQuery = """SELECT
                         actual_loop_id,
                         time,
                         vcc,
                         channel,
                         active_passive_reply,
                         actual_count_direction,
                         actual_velocity,
                         emergency_brakes_status,
                         operating_mode,
                         position_number,
                         request_type1_response,
                         train_integrity,
                         vehicle_door_status
                       FROM """ + keyspace + "." + table + """ WHERE
                         date=? AND
                         vehicle_id_command=? AND
                         actual_loop_id=? AND
                         time<=?
                       LIMIT 1"""
      selectQuery
  }

  case class PollModel(actual_loop_id: Int,
                       time: Date,
                       vcc: Int,
                       channel: Int,
                       active_passive_reply: Int,
                       actual_count_direction: Int,
                       actual_velocity: Int,
                       emergency_brakes_status: Int,
                       operating_mode: Int,
                       position_number: Int,
                       request_type1_response: Int,
                       train_integrity: Int,
                       vehicle_door_status: Int )

  def extract_values(e: PollModel): Array[Any] = e.productIterator.map {
    case op: Option[_] => op.getOrElse(null)
    case v             => v
  }.toArray

  def cql_session( cl:String, ks:String ) : ScalaSession = {
    val cluster = Cluster.builder.addContactPoint(cl).build()
    val session = cluster.connect()
    val sSession = ScalaSession(ks)(session)
    sSession
  }

  val DateFormatter = new SimpleDateFormat("yyyy-MM-dd")

  def date_to_time( date:Date ) : Timestamp = {
    val time:Timestamp = new Timestamp(date.getTime())
    time
  }

  def query_row( session:ScalaSession, query:String, str_date:String, str_vhid:Int, str_loop_id:Int, str_time:String ) : Iterator[Row] = {
    val time:Date = DateTime.parse(str_time).toDate
    val date:Date = DateFormatter.parse(str_date)
    val localdate:LocalDate = LocalDate.fromMillisSinceEpoch(date.getTime())
    session.rawSelect(query, localdate, Int.box(str_vhid), Int.box(str_loop_id), time)
  }

  def get_row( session:ScalaSession, query:String, str_date:String, int_vhid:Int, int_loop_id:Int, str_time:String ) : Array[Any] = {
    val aRow: Row = query_row(session, query:String, str_date, int_vhid, int_loop_id, str_time).next()  // LIMIT 1 query, select next
    val model: PollModel = aRow.as[PollModel]
    extract_values(model)
  }

}

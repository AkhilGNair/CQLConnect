package CQLConnector

import scala.language.implicitConversions
import org.joda.time.DateTime
import com.datastax.driver.core.{ Cluster, Row }
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
                       FROM """ + keyspace + "." + table + """WHERE
                         date=? AND
                         vehicle_id_command=? AND
                         actual_loop_id=? AND
                         time<=?
                       LIMIT 1"""
      selectQuery
  }

  case class PollModel(actual_loop_id: Int,
                       time: java.util.Date,
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

  implicit def run(e: PollModel): Array[Any] = e.productIterator.map {
    case op: Option[_] => op.getOrElse(null)
    case v             => v
  }.toArray

  def cql_session( cl:String, ks:String ) : ScalaSession = {
    val cluster = Cluster.builder.addContactPoint(cl).build()
    val session = cluster.connect()
    val sSession = ScalaSession(ks)(session)
    sSession
  }

  def query_row( session:ScalaSession, query:String, str_date:String, str_vhid:String, str_loop_id:String, str_time:String ) : Iterator[Row] = {
    val time:java.util.Date = DateTime.parse(str_time).toDate
    session.rawSelect(query, str_date, str_vhid, str_loop_id, str_time)
  }

  def get_row( session:ScalaSession, query:String, str_date:String, str_vhid:String, str_loop_id:String, str_time:String ) : Array[Any] = {
    val aRow = query_row(session, query:String, str_date, str_vhid, str_loop_id, str_time).next()
    val values: PollModel = aRow.as[PollModel]
    values
  }

}

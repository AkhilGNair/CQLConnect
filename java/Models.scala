package CQLConnect

import java.sql
import java.util.Date

import com.datastax.driver.core.LocalDate
import com.weather.scalacass.ScalaSession

import CQLConnect.DateUtils.convert_cld_to_d


object Models {

  abstract class CassandraModel() extends Product

  case class PollModel(
    actual_loop_id: Int,
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
    vehicle_door_status: Int
  ) extends CassandraModel


  case class NullResponseModel(
    line: Int,
    date: LocalDate,
    time: Date,
    vehicle_id_command: Int,
    vcc: Int,
    channel: Int,
    command_response_type: String
  ) extends CassandraModel


  case class DistMVFileIndexModel(
    line: Int,
    analysed: Boolean,
    date: LocalDate,
    file_name: String,
    state: String
  ) extends CassandraModel


 def extract(e: CassandraModel): Array[Any] = e.productIterator.map {

    case ld: LocalDate => ld: Date
    case op: Option[_] => op.getOrElse(null)
    case v => v

  }.toArray

}

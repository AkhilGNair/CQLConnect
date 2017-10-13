package CQLConnect

import com.weather.scalacass.ScalaSession

object Queries {

  def previous_poll_query( keyspace: String, table: String ) : String = {

    val previous_poll_query =
      s"""SELECT
         |  actual_loop_id,
         |  time,
         |  vcc,
         |  channel,
         |  active_passive_reply,
         |  actual_count_direction,
         |  actual_velocity,
         |  emergency_brakes_status,
         |  operating_mode,
         |  position_number,
         |  request_type1_response,
         |  train_integrity,
         |  vehicle_door_status
         |FROM $keyspace.$table WHERE
         |  line=? AND
         |  date=? AND
         |  vehicle_id_command=? AND
         |  actual_loop_id=? AND
         |  time<=?
         |  LIMIT 1""".stripMargin

    previous_poll_query

  }

  def null_response_query( session: ScalaSession ) : String = {

    val keyspace = session.keyspace

    val null_response_query =
      s"""SELECT
       |  *
       |FROM $keyspace.polls_ordered WHERE
       |  line=? AND
       |  date=? AND
       |  vehicle_id_command=?
       |""".stripMargin

    null_response_query

  }

}

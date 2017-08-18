package CQLConnect

import com.weather.scalacass.ScalaSession


object Count {

  def get_count(session: ScalaSession, table: String): Long = {

    val keyspace = session.keyspace

    val count_query =
      s"""SELECT
         |  count(*)
         |FROM $keyspace.$table
         |""".stripMargin

    session.rawSelect(count_query).next().getLong("count")

  }

}

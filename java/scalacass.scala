package CQLScalaCass

import com.datastax.driver.core.{ Cluster, Row }
import com.weather.scalacass._
import com.weather.scalacass.syntax._

object CQL {

  def cql_session( cl:String, ks:String ) : ScalaSession = {
    val cluster = Cluster.builder.addContactPoint(cl).build()
    val session = cluster.connect()
    val sSession = ScalaSession(ks)(session)
    sSession
  }

  def query_row( session:ScalaSession, table:String ) : Option[Row] = session.selectOne(table, ScalaSession.NoQuery())

  def get_row( session:ScalaSession, table:String ) : Array[Any] = {
    val aRow = query_row(session, table).get
    Array(aRow.getUUID(0), aRow.getInt(1), aRow.getString(2), aRow.getString(3), aRow.getUUID(4),  aRow.getString(5))
  }

}

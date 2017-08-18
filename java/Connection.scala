package CQLConnect

import com.datastax.driver.core.Cluster
import com.weather.scalacass.ScalaSession


object Connection {

  def cql_cluster_connect( cl:String ) : Cluster = {
    val cluster = Cluster.builder.addContactPoint(cl).build()
    cluster
  }

  def cql_session_connect( cl:Cluster, ks:String ) : ScalaSession = {
    val session_manager = cl.connect()
    val session = ScalaSession(ks)(session_manager)
    session
  }

  def cql_session_close( session:ScalaSession ) : Unit = {
    session.close()
  }

  def cql_cluster_close( cl:Cluster) : Unit = {
    cl.close()
  }

}

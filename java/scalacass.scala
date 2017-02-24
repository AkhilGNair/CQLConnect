import com.weather.scalacass._
import com.datastax.driver.core.{ Cluster, Row }

object thing {

  implicit val cluster = Cluster.builder.addContactPoint("localhost").build()
  implicit val session = cluster.connect()

  val sSession = ScalaSession("spark")(session) // if mykeyspace already exists

  def test( a:Any* ) : Iterator[Row] = {
    // given the table and query definition
    case class MyTable(s: String, i: Int, l: Option[Long])
    case class Query(s: String, i: Int)

    val selectRes: Iterator[Row] = sSession.select("mytable", Query("asdf", 123), false, Option(1L))
  }

}

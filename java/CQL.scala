package CQLStrings

// All rough work trying without scalacass!

import com.datastax.driver.core._
import shade.com.datastax.spark.connector.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture }
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions
import scala.collection.JavaConverters._


object CQL {

  implicit val session = new Cluster
      .Builder()
      .addContactPoints("localhost")
      .withPort(9042)
      .build()
      .connect()

  implicit class CqlStrings(val context: StringContext) extends AnyVal {
    def cql(args: Any*)(implicit session: Session): Future[PreparedStatement] = {
      val statement = new SimpleStatement(context.raw(args: _*))
      session.prepareAsync(statement)
    }
  }

  implicit def listenableFutureToFuture[T](
    listenableFuture: ListenableFuture[T]
  ): Future[T] = {
    val promise = Promise[T]()
    Futures.addCallback(listenableFuture, new FutureCallback[T] {
      def onFailure(error: Throwable): Unit = {
        promise.failure(error)
        ()
      }
      def onSuccess(result: T): Unit = {
        promise.success(result)
        ()
      }
    })
    promise.future
  }

  def execute(statement: Future[PreparedStatement], params: Any*)(
    implicit executionContext: ExecutionContext, session: Session
  ): Future[ResultSet] =
    statement
      .map(_.bind(params.map(_.asInstanceOf[Object])))
      .flatMap(session.executeAsync(_))

/*  def execute_cql( a:String ) : Future[Iterable[Row]] = {
    val myKey = 1
    val resultSet = execute(
       cql"select state, path from spark.file_index limit ?",
       myKey
    )*/

  def get_state( r:Row ) : String = {
    r.getString("state")
  }

  def execute_cql( a:String ) : Future[Iterable[Row]] = {
    val myKey = 1
    val resultSet = execute(
       cql"select state, path from spark.file_index limit ?",
       myKey
    )

    val rows: Future[Iterable[Row]] = resultSet.map(_.asScala)
    rows
  }



}

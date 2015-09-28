import akka.actor._
import akka.routing._
import Source._

case class StartMessage(file: String)
case class StartResult(result: Seq[(String, String, String)])

case class DBMessage(jdbc: String)
case class DBResult(values: Seq[(String, String)])

case class ColumnMessage(jdbc: String, column:Column)
case class ColumnResult(column: Column, value: Option[String])

object SourceActorObject {

  class ListenerActor(explorer: ActorRef) extends Actor {
    def receive = {
      case StartMessage(file) => {
        getJDBCConnectionsFromFile(file) foreach { jdbc =>
          explorer !  DBMessage(jdbc)
        }
      }
    }
  }

  class ConnectionExploreActor(extractor: ActorRef) extends Actor {
    val visitedColumns = collection.mutable.Set.empty[Column]

    def isColumnAllowed(column: Column) = !Source.isDuplicateColumn(column, visitedColumns)

    def receive = {
      case DBMessage(jdbc: String) => {
        val conn = createJDBCConnection(jdbc)
        val columns = getAllColumns(conn)
        conn.close
        println(s"Exploring $jdbc")
        columns filter isTimeColumn filter isColumnAllowed foreach (extractor ! ColumnMessage(jdbc, _))
      }
    }
  }

  class ValueExtractorActor extends Actor {
    def receive = {
      case ColumnMessage(jdbc: String, column:Column) => {
        val conn = createJDBCConnection(jdbc)
        getValue(conn, column) match {
          case Some(value) => println(s"Got $value from table ${column.table} column ${column.name}")
          case None => println(s"Failed to get value from table ${column.table} column ${column.name}")
        }
        conn.close
      }
    }
  }


  def main (args: Array[String]) {
    val system = ActorSystem("ebi-sources")
    val extractorActor = system.actorOf(RoundRobinPool(5).props(Props[ValueExtractorActor]))
    val exploreActor = system.actorOf(Props(new ConnectionExploreActor(extractorActor)))
    val inputActor = system.actorOf(Props(new ListenerActor(exploreActor)))

    val path = "ebi-sources.json"
    inputActor ! StartMessage(path)
  }
}



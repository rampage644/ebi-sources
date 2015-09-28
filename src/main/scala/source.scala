import java.sql.{DatabaseMetaData, DriverManager, Connection}

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.{Try, Success, Failure}

object Source {
  case class Column(name:String, ctype:String, table:String, schema:String, catalog:String)

  def getJDBCConnectionsFromFile(path: String): Seq[String] = {
    val contents = io.Source.fromFile(path).mkString
    val json = parse(contents)
    // get all possible jdbc strings
    for {JString(jdbc) <- json \\ "jdbc"} yield jdbc
  }

  def getBlacklistConnectionsFromFile(path: String): Seq[String] = {
    Try(io.Source.fromFile(path).getLines().toSeq) match {
      case Success(result) => result
      case Failure(e) => Seq()
    }
  }

  def isDuplicateColumn(column: Column, visitedColumns: collection.mutable.Set[Column]) = {
    if (visitedColumns contains column) true else {
      visitedColumns add column
      false
    }
  }

  def isTimeColumn(column: Column) =
    column.ctype == "DATE" ||
      column.ctype == "DATETIME" ||
      column.ctype == "TIMESTAMP" ||
      column.ctype == "TIME_WITH_TIMEZONE" ||
      column.ctype == "TIMESTAMP_WITH_TIMEZONE"


  def main(args: Array[String]) {

    val path = "/home/ramp/Downloads/ebi-sources_1.json"
    val blacklist = "blacklist.txt"

    val (workingConnections, failedConnections) = getJDBCConnectionsFromFile(path)
      .filter(conn => !getBlacklistConnectionsFromFile(blacklist).contains(conn))
      .partition(c => Try(createJDBCConnection(c)).isSuccess)

    println("   Failed connections: ")
    failedConnections foreach println
    println("-" * 76)

    println("   Successful connections: ")
    workingConnections foreach println
    println("-" * 76)


    val time_columns = Set("DATE", "DATETIME", "TIMESTAMP", "TIME_WITH_TIMEZONE" ,"TIMESTAMP_WITH_TIMEZONE")

    val visitedColumns = collection.mutable.Set[Column]()
    def filterColumn(column: Column, visited: collection.mutable.Set[Column]): Boolean = {
      if (visited contains column) false else { visited.synchronized(visited add column); true }
    }

    (for {
      jdbcConnection <- workingConnections.toIterable.par
      conn = createJDBCConnection(jdbcConnection)
      column <- getAllColumns(conn) if filterColumn(column, visitedColumns) && time_columns.contains(column.ctype)
      value <- getValue(conn, column)
    } yield (jdbcConnection, column, value)) foreach println
  }


  def getAllColumns(conn: Connection): Iterator[Column] = {
    val tryRs = Try(conn.getMetaData.getColumns(null, null, "%", "%"))
    tryRs match {
      case Failure(e) => Iterator.empty
      case Success(rs) => Iterator.continually((rs.next, rs)).takeWhile(_._1).map {
        case (_,rs) => Column(rs.getString(4), rs.getString(6), rs.getString(3), rs.getString(2), rs.getString(1))
      }
    }
  }

  def getValue(conn: Connection, column: Column): Option[String] = {
    val stmt = conn.createStatement()
    val table = column match {
      case Column(_,_,table,null,_) => table
      case Column(_,_,table,schema,_) => s"$schema.$table"
    }
    val stmtString = s"SELECT max(${column.name}) from ${table}"
    val tryResults = Try(stmt.executeQuery(stmtString))
    tryResults match {
      case Success(rs) => { rs.next; Option(rs.getString(1)) }
      case Failure(e) => None
    }
  }

  def createJDBCConnection(jdbcConnectionString: String) = {
    val driver = jdbcConnectionString match {
      case d if d.contains("sqlserver") => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case d if d.contains("mysql") => "com.mysql.jdbc.Driver"
      case d if d.contains("oracle") => "oracle.jdbc.OracleDriver"
      case d if d.contains("postgresql") => "org.postgresql.Driver"
      case _ => ""
    }

    Class.forName(driver)
    DriverManager.getConnection(jdbcConnectionString)
  }
}


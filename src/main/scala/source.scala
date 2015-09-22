import java.sql.{DatabaseMetaData, DriverManager, Connection}

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.{Failure, Success, Try}

object Source {

  def main(args: Array[String]) {
    val path = ""
    val contents = io.Source.fromFile(path).mkString
    val json = parse(contents)
    // get all possible jdbc strings
    val jdbcConnectionList = for {JString(jdbc1) <- json \\ "jdbc"} yield jdbc1

    val time_columns = Set("DATE", "DATETIME", "TIMESTAMP", "TIME_WITH_TIMEZONE" ,"TIMESTAMP_WITH_TIMEZONE")

    for {
      jdbcConnection <- jdbcConnectionList.drop(4).take(1)
      conn = createJDBCConnection(jdbcConnection)
      column <- getAllColumns(conn) if time_columns contains column.ctype
      value <- getValue(conn, column)
    } yield (jdbcConnection, column, value)
  }

    case class Column(name:String, ctype:String, table:String, schema:String, catalog:String)

  def getAllColumns(conn: Connection): Seq[Column] = {
    val rs = conn.getMetaData.getColumns(null, null, "%", "%")

    Iterator.continually((rs.next, rs)).takeWhile(_._1).map {
      case (_,rs) => Column(rs.getString(4), rs.getString(6), rs.getString(3), rs.getString(2), rs.getString(1))
    } toSeq
  }

  def getValue(conn: Connection, column: Column): String = {
    val stmt = conn.createStatement()
    val stmtString = s"SELECT max(${column.name}) from ${column.schema}.${column.table}"
    println(s"    ${column.name}: ${column.ctype}, query: $stmtString")
    val rs = stmt.executeQuery(stmtString)
    rs.next
    val res = rs.getString(1)
    println(s"  ${rs.getString(1)}")
    if (res != null) res else ""
  }

  def createJDBCConnection(jdbcConnectionString: String) = {
    println(jdbcConnectionString)

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


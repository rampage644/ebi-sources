import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, temporal}

import scala.reflect.io.{Path, File}
import scala.xml.XML
import scala.xml.dtd.{DocType, PublicID}

/**
 * Created by ramp on 9/29/15.
 */
object ResultGenerator {
  def main(args: Array[String]) {
    val path = "result.txt"
    val it = io.Source.fromFile(path).getLines map readLine map {
      case (jdbc: String, column: String, value: String) => (connectionStringName(jdbc), column.toLowerCase, value, score(value))
    } toList


    val output =
    <html>
      <head>
        <meta charset="UTF-8" />
        <title>Connection availablity</title>
        <style>
          td.red {{
            color: red;
          }}
          td.yellow {{
            color: yellow;
          }}
          td.green {{
            color: green;
          }}
        </style>
      </head>
      <body>
        <table>
          <tr>
            <th>Connection</th>
            <th>Column</th>
            <th>Value</th>
            <th>Score</th>
          </tr>{it.sortWith(_._1 < _._1).map {
          case (conn: String, column: String, value: String, score: Long) =>
            <tr>
              <td>{conn}</td>
              <td>{column}</td>
              <td>{value}</td>
              <td class={score match {
                case n if n > 0 &&  n <= 30 => "green"
                case n if n > 30 && n <= 183 => "yellow"
                case _ => "red"
              }
                  }>{score}</td>
            </tr>
        }}
        </table>
      </body>
    </html>

    val doctype = DocType("html",
      PublicID("-//W3C//DTD XHTML 1.0 Strict//EN",
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"),
      Nil)
    XML.save("result.html", output, "UTF-8", false, doctype)
  }

    val re = """\((.*),Column\((.*)\),(.*)\)""".r
  def readLine(line:String):(String,String,String) = {
    def createTableColumnString(input:String) = input.split(',') match {
      case Array(column, _, table, "null", _*) => s"$table.$column"
      case Array(column, _, table, schema, _*) => s"$schema.$table.$column"
    }

    re.findFirstMatchIn(line) match {
      case Some(m) => (m.group(1), createTableColumnString(m.group(2)) , m.group(3))
      case None => ("", "", "")
    }
  }

  def score(timestamp: String, ref: LocalDate = LocalDate.now): Long = {
    temporal.ChronoUnit.DAYS.between (
      timestamp.length match {
        case 21 => LocalDateTime.parse (timestamp, DateTimeFormatter.ofPattern ("yyyy-MM-dd HH:mm:ss.S") ).toLocalDate
        case 10 => LocalDate.parse (timestamp, DateTimeFormatter.ofPattern ("yyyy-MM-dd"))
        case _ => ref
      },
      ref)
  }

  def connectionStringName(jdbc:String): String = {
    jdbc match {
      case input if input.contains("oracle") => s"[ORACLE] ${input.split('@')(1)}"
      case input if input.contains("mysql") => s"[MYSQL] ${input.split('?')(0).split("://")(1)}"
      case n => n
    }
  }

}

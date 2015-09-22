import Source._
import org.scalatest.{FunSuite, Matchers}

class TestSource extends FunSuite with Matchers {
  test("Getting values") {
    val string = "jdbc:oracle:thin:user/passwd@host.com:1521/sitname"
    val conn = createJDBCConnection(string)
    val value = getValue(conn, Column("LAST_UPDATED_ON","DATE","APEX_APPLICATIONS","APEX_030200",null))
    assert (value != null)
  }

}

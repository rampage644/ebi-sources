import org.scalatest.{FunSuite, Matchers}
import ResultGenerator._

class ResultGeneratorTest extends FunSuite with Matchers {
  test("readLine should correctly find elements") {
    val input = "(jdbc:oracle:thin:us/pass@ebiinfo-scan.corp.rackspace.com:1521/infoprd.corp.rackspace.com,Column(CREATED_AT,DATE,ODS_INFORMATICA_SESSIONS_BKP,EDWDEVUSER,null),2013-09-08 01:37:36.0)"
    readLine(input) should be (("jdbc:oracle:thin:us/pass@ebiinfo-scan.corp.rackspace.com:1521/infoprd.corp.rackspace.com",
      "EDWDEVUSER.ODS_INFORMATICA_SESSIONS_BKP.CREATED_AT", "2013-09-08 01:37:36.0"))

    val input2 = "(jdbc:mysql://10.9.152.83:3306/loadbalancing?user=us&password=pass,Column(created,TIMESTAMP,node_service_event,null,loadbalancing),2015-09-28 16:34:16.0)"
    readLine(input2) should be (("jdbc:mysql://10.9.152.83:3306/loadbalancing?user=us&password=pass",
      "node_service_event.created", "2015-09-28 16:34:16.0"))
  }

  test("score should return score close to zero for recent timestamps") {
    val input = "2015-09-28 16:34:16.0"
    assert (score(input) > 0)
  }
}

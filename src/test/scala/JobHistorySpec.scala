import TestSparkContext._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class JobHistorySpec extends AnyFlatSpec {

  "Job history app" should "be able to read json data from a path" in {
    val path = "src/main/resources/job-data"
    val jobDS = JobHistoryApp.readJsonData(path)(sparkSession)
    jobDS.isEmpty should be(false)
  }

  it should "return record count for the json data" in {
    val path = "src/main/resources/job-data"
    val jobsRecordCount = JobHistoryApp.getRecordCount(path)(sparkSession)
    jobsRecordCount should be(17139693)
  }
}

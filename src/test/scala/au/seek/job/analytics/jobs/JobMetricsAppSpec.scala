package au.seek.job.analytics.jobs

import au.seek.job.analytics.util.TestSparkContext.sparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class JobMetricsAppSpec extends AnyFlatSpec {

  private val inputDataPath = "src/main/resources/job-data/"
  private val writePath = ""

  "Job history app" should "be able to read json data from a path" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    jobDS.isEmpty should be(false)
  }

  it should "return record count for the json data" in {
    val jobsRecordCount = JobMetricsApp.getRecordCount(inputDataPath)(sparkSession)
    jobsRecordCount should be(17139693)
  }

  it should "return average salary per profile" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getAvgSalaryPerProfile(topN = 10)(jobDS)
    actual.show(truncate = false)
  }

  it should "return average salary across all profiles" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getAvgSalary(jobDS)
    actual.show(truncate = false)
  }

  it should "return top N paying job titles" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    JobMetricsApp.getTopNPayingJobs(topN = 5)(jobDS)
  }

  it should "return bottom N paying job titles" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getBottomNPayingJobs(bottomN = 5)(jobDS)
    actual.show(truncate = false)
  }

  it should "return number of people currently working" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getNumEmployedCurrently(jobDS)
    println(actual)
  }

  it should "return latest job per profile" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getLatestJobPerProfile(jobDS)
    actual.show(truncate = false)
  }

  it should "return highest paying job per person" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getHighestPayingJobPerProfile(jobDS)
    actual.show(truncate = false)
  }

  it should "write highest paying job per person partitioned by year" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    JobMetricsApp
      .writeHighestPayingJobPerProfile(writePath)(jobDS)
  }

  it should "return the current top earner details" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.currentTopEarner(jobDS)
    actual.show(truncate = false)
  }

  it should "return the most popular job title for a given year" in {
    val jobDS = JobMetricsApp.readJsonData(inputDataPath)(sparkSession)
    val actual = JobMetricsApp.getMostPopularJobTitleForYear(targetYear = 2019)(jobDS)
    actual.show(truncate = false)
  }
}

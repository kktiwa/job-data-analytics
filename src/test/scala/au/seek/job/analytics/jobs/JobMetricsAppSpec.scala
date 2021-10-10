package au.seek.job.analytics.jobs

import au.seek.job.analytics.util.TestSparkContext.sparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class JobMetricsAppSpec extends AnyFlatSpec {

  "Job history app" should "be able to read json data from a path" in {
    val path = "src/main/resources/job-data/part0.json"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    jobDS.isEmpty should be(false)
  }

  it should "return record count for the json data" in {
    val path = "src/main/resources/job-data"
    val jobsRecordCount = JobMetricsApp.getRecordCount(path)(sparkSession)
    jobsRecordCount should be(17139693)
  }

  it should "return average salary per profile" in {
    val path = "src/main/resources/job-data"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    JobMetricsApp.getAvgSalaryPerProfile(topN = 10)(jobDS)
  }

  it should "return average salary across all profiles" in {
    val path = "src/main/resources/job-data/part0.json"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    val actual = JobMetricsApp.getAvgSalary(jobDS)
  }

  it should "return top N paying job titles" in {
    val path = "src/main/resources/job-data/"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    val actual = JobMetricsApp.getTopNPayingJobs(topN = 5)(jobDS)
  }

  it should "return bottom N paying job titles" in {
    val path = "src/main/resources/job-data/"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    val actual = JobMetricsApp.getBottomNPayingJobs(bottomN = 5)(jobDS)
  }

  it should "return number of people currently working" in {
    val path = "src/main/resources/job-data"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    val actual = JobMetricsApp.getNumEmployedCurrently(jobDS)
  }

  it should "return latest job per profile" in {
    val path = "src/main/resources/job-data/part0.json"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    JobMetricsApp.getLatestJobPerProfile(jobDS)
  }

  it should "return highest paying job per person" in {
    val path = "src/main/resources/job-data/part0.json"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    JobMetricsApp.getHighestPayingJobPerProfile(jobDS)
  }

  it should "write highest paying job per person partitioned by year" in {
    val path = "src/main/resources/job-data/part0.json"
    val jobDS = JobMetricsApp.readJsonData(path)(sparkSession)
    JobMetricsApp
      .writeHighestPayingJobPerProfile("/Users/kunal.tiwary/personal/job-data-analytics/output-path/maxSalariesByYear.parquet")(jobDS)
  }
}

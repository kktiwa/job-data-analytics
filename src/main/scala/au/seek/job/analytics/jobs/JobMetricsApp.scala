package au.seek.job.analytics.jobs

import au.seek.job.analytics.models.{Job, JobHistory, JobHistoryWithProfileRow, JobProfile}
import org.apache.spark.sql.expressions.Window
import au.seek.job.analytics.util.SparkContextProvider._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, SparkSession}

object JobMetricsApp extends App {

  import spark.implicits._

  private val dateFormat = "yyyy-MM-dd"

  /**
   * Read JSON data from a given path
   *
   * @param filePath
   * @param sparkSession
   * @return
   */
  def readJsonData(filePath: String)
                  (sparkSession: SparkSession): Dataset[Job] = {
    import sparkSession.implicits.newProductEncoder
    sparkSession.read.format("json").load(filePath).as[Job]
  }

  /**
   * Return the count of rows in the given JSON data
   *
   * @param filePath
   * @param sparkSession
   * @return
   */
  def getRecordCount(filePath: String)
                    (sparkSession: SparkSession): Long = {
    readJsonData(filePath)(sparkSession).count()
  }

  /**
   *
   * @param topN
   * @param ds
   * @return First `topN` results ordered by lastName in descending order
   */
  def getAvgSalaryPerProfile(topN: Int)
                            (ds: Dataset[Job]): DataFrame = {
    val windowSpec = Window.partitionBy(JobHistoryWithProfileRow.id)
    explodeJobProfile(ds)
      .select(
        col(JobHistoryWithProfileRow.id),
        col(JobHistoryWithProfileRow.lastName),
        avg(JobHistoryWithProfileRow.salary) over windowSpec as "avg_salary"
      ).dropDuplicates()
      .limit(topN)
      .orderBy(desc(JobProfile.lastName))
  }

  /**
   *
   * @param ds
   * @return average salary across all job profiles
   */
  def getAvgSalary(ds: Dataset[Job]): DataFrame = {
    explodeJobProfile(ds)
      .select(avg(JobHistoryWithProfileRow.salary) as "avg_salary")
  }

  /**
   *
   * @param topN
   * @param ds
   * @return Top `topN` paying jobs. If tie order by title,location
   */
  def getTopNPayingJobs(topN: Int)
                       (ds: Dataset[Job]): DataFrame = {
    val windowSpec = Window
      .partitionBy(col(JobHistory.title))

    val rankSpec = Window
      .orderBy(
        desc("avg_salary_by_job_title"),
        col(JobHistory.location),
        col(JobHistory.title)
      )

    explodeJobProfile(ds)
      .select(
        col(JobHistory.title),
        col(JobHistory.location),
        avg(JobHistory.salary) over windowSpec as "avg_salary_by_job_title"
      ).dropDuplicates()
      .select(
        col(JobHistory.title),
        col(JobHistory.location),
        col("avg_salary_by_job_title"),
        dense_rank() over rankSpec as "rank"
      ).limit(topN)
  }

  /**
   *
   * @param bottomN
   * @param ds
   * @return Bottom `bottomN` paying jobs. If tie order by title,location
   */
  def getBottomNPayingJobs(bottomN: Int)
                          (ds: Dataset[Job]): DataFrame = {
    val windowSpec = Window
      .partitionBy(col(JobHistory.title))

    val rankSpec = Window
      .orderBy(
        asc("avg_salary_by_job_title"),
        col(JobHistory.location),
        col(JobHistory.title)
      )

    explodeJobProfile(ds)
      .select(
        col(JobHistoryWithProfileRow.title),
        col(JobHistoryWithProfileRow.location),
        avg(JobHistoryWithProfileRow.salary) over windowSpec as "avg_salary_by_job_title"
      ).dropDuplicates()
      .select(
        col(JobHistoryWithProfileRow.title),
        col(JobHistoryWithProfileRow.location),
        col("avg_salary_by_job_title"),
        dense_rank() over rankSpec as "rank"
      ).limit(bottomN)
  }

  /**
   * Highest earner job profile. If tie, order by lastName descending, fromDate descending.
   *
   * @param ds
   * @return
   */
  def currentTopEarner(ds: Dataset[Job]): DataFrame = {
    val windowSpec = Window
      //inorder to consider ALL rows in the partition
      .partitionBy(col(JobHistoryWithProfileRow.toDate))
      .orderBy(
        desc(JobHistoryWithProfileRow.salary),
        desc(JobHistoryWithProfileRow.lastName),
        desc(JobHistoryWithProfileRow.firstName)
      )

    explodeJobProfile(ds)
      //only select profiles where someone is currently working
      .filter(x => x.fromDate.nonEmpty && x.toDate.isEmpty)
      .select(
        col(JobHistoryWithProfileRow.id),
        col(JobHistoryWithProfileRow.firstName),
        col(JobHistoryWithProfileRow.lastName),
        col(JobHistoryWithProfileRow.salary),
        col(JobHistoryWithProfileRow.fromDate),
        col(JobHistoryWithProfileRow.toDate),
        rank() over windowSpec as "salary_row_num"
      ).filter(col("salary_row_num") === 1)
  }


  /**
   * Most popular job title for a given year
   *
   * @param targetYear
   * @param ds
   * @return
   */
  def getMostPopularJobTitleForYear(targetYear: Int)
                                   (ds: Dataset[Job]): DataFrame = {
    explodeJobProfile(ds)
      .select(
        col(JobHistoryWithProfileRow.title),
        year(formatDateColumn(JobHistoryWithProfileRow.fromDate)) as "year"
      )
      .filter(col("year") === targetYear)
      .groupBy(col(JobHistoryWithProfileRow.title))
      .agg(
        count(col(JobHistoryWithProfileRow.title)) as "job_title_count"
      )
      .orderBy(desc("job_title_count"))
      .limit(1)
  }


  /**
   *
   * @param ds
   * @return Number of people currently working
   */
  def getNumEmployedCurrently(ds: Dataset[Job]): Long = {
    ds.map { x =>
      x.profile.jobHistory match {
        //currently working/employed
        case Some(jh) => jh.exists(_.toDate.isEmpty)
        //not working/unemployed
        case None => false
      }
    }.filter(col("value") === true)
      .count()
  }

  /**
   *
   * @param ds
   * @return List latest job. Display the first 10 results, ordered by lastName descending, firstName ascending order.
   */
  def getLatestJobPerProfile(ds: Dataset[Job]): DataFrame = {
    val windowSpec = Window.partitionBy(JobHistoryWithProfileRow.id)
      .orderBy(desc("fromDate_formatted"))
    explodeJobProfile(ds)
      .withColumn("fromDate_formatted",
        formatDateColumn(JobHistoryWithProfileRow.fromDate)
      )
      .select(
        col(JobHistoryWithProfileRow.firstName),
        col(JobHistoryWithProfileRow.lastName),
        col(JobHistoryWithProfileRow.title),
        col(JobHistoryWithProfileRow.fromDate),
        row_number() over windowSpec as "job_date_rank"
      ).filter(col("job_date_rank") === 1)
      .orderBy(
        desc(JobHistoryWithProfileRow.lastName), asc(JobHistoryWithProfileRow.firstName)
      ).limit(10)
  }

  /**
   *
   * @param ds
   * @return
   * For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary.
   * Store the results in a dataframe, and then print out 10 results
   */
  def getHighestPayingJobPerProfile(ds: Dataset[Job]): DataFrame = {
    val windowSpec = Window.partitionBy(Job.id)
      .orderBy(desc(JobHistoryWithProfileRow.salary))

    explodeJobProfile(ds)
      .select(
        col(JobHistoryWithProfileRow.firstName),
        col(JobHistoryWithProfileRow.lastName),
        col(JobHistoryWithProfileRow.toDate),
        col(JobHistoryWithProfileRow.fromDate),
        col(JobHistoryWithProfileRow.salary),
        rank() over windowSpec as "salary_row_num"
      ).filter(col("salary_row_num") === 1)
      .select(
        col(JobHistoryWithProfileRow.firstName),
        col(JobHistoryWithProfileRow.lastName),
        col(JobHistoryWithProfileRow.salary),
        //get the latest year (toDate) value when they made the max salary
        //if toDate is null (i.e. they still work at that job), get the fromDate
        year(
          coalesce(
            formatDateColumn(JobHistoryWithProfileRow.toDate),
            formatDateColumn(JobHistoryWithProfileRow.fromDate)
          )
        ) as "year"
      ).orderBy("year")
  }

  /**
   *
   * @param ds
   * @return Write compressed, partitioned by year of their highest paying job
   */
  def writeHighestPayingJobPerProfile(writePath: String)
                                     (ds: Dataset[Job]): Unit = {
    getHighestPayingJobPerProfile(ds)
      .repartition(col("year"))
      .write
      .partitionBy("year")
      //these should be set via config
      .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .parquet(writePath)
  }

  private def explodeJobProfile(ds: Dataset[Job]): Dataset[JobHistoryWithProfileRow] = {
    ds
      .select(
        $"id" as Job.id,
        $"profile.firstName" as JobProfile.firstName,
        $"profile.lastName" as JobProfile.lastName,
        explode($"profile.jobHistory") as JobProfile.jobHistory
      ).select(
      col(JobHistoryWithProfileRow.id) as JobHistoryWithProfileRow.id,
      col(JobHistoryWithProfileRow.firstName) as JobHistoryWithProfileRow.firstName,
      col(JobHistoryWithProfileRow.lastName) as JobHistoryWithProfileRow.lastName,
      $"jobHistory.title" as JobHistoryWithProfileRow.title,
      $"jobHistory.location" as JobHistoryWithProfileRow.location,
      $"jobHistory.salary" as JobHistoryWithProfileRow.salary,
      $"jobHistory.fromDate" as JobHistoryWithProfileRow.fromDate,
      $"jobHistory.toDate" as JobHistoryWithProfileRow.toDate
    ).as[JobHistoryWithProfileRow]
  }

  private def formatDateColumn(dateColName: String): Column = {
    to_date(col(dateColName), dateFormat)
  }

}

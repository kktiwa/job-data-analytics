package au.seek.job.analytics.util

import org.apache.spark.sql.SparkSession

object SparkContextProvider {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Job Profile Analytics")
    .getOrCreate

}

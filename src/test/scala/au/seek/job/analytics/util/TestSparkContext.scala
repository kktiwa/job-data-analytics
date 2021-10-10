package au.seek.job.analytics.util

import org.apache.spark.sql.SparkSession

object TestSparkContext {

  lazy val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

}

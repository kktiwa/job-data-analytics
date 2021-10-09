import org.apache.spark.sql.SparkSession

object SparkContextProvider {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Job Data Analytics")
    .getOrCreate

}

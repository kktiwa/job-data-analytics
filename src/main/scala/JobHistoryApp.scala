
import org.apache.spark.sql.{Dataset, SparkSession}

object JobHistoryApp extends App {

  def readJsonData(filePath: String)
                  (sparkSession: SparkSession): Dataset[Job] = {
    import sparkSession.implicits.newProductEncoder
    sparkSession.read.format("json").load(filePath).as[Job]
  }

  def getRecordCount(filePath:String)
                    (sparkSession: SparkSession): Long = {
    readJsonData(filePath)(sparkSession).count()
  }

}

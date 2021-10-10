package au.seek.job.analytics.models

case class JobHistory(title: String,
                      location: String,
                      salary: BigDecimal,
                      fromDate: String,
                      toDate: Option[String] = None
                     )

object JobHistory {

  val title = "title"
  val location = "location"
  val salary = "salary"
  val fromDate = "fromDate"
  val toDate = "toDate"
}

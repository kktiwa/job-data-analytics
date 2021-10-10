package au.seek.job.analytics.models

case class JobHistoryWithProfileRow(id: String,
                                    firstName: String,
                                    lastName: String,
                                    title: String,
                                    location: String,
                                    salary: BigDecimal,
                                    fromDate: String,
                                    toDate: Option[String] = None
                                   )


object JobHistoryWithProfileRow {

  val id = Job.id
  val firstName = JobProfile.firstName
  val lastName = JobProfile.lastName
  val title = JobHistory.title
  val location = JobHistory.location
  val salary = JobHistory.salary
  val fromDate = JobHistory.fromDate
  val toDate = JobHistory.toDate
}
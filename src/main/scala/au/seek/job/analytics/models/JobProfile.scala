package au.seek.job.analytics.models

case class JobProfile(firstName: String,
                      lastName: String,
                      jobHistory: Option[Seq[JobHistory]] = None
                     )

object JobProfile {

  val firstName = "firstName"
  val lastName = "lastName"
  val jobHistory = "jobHistory"
}

package au.seek.job.analytics.models

case class Job(id: String,
               profile: JobProfile
              )

object Job {
  val id = "id"
  val profile = "profile"
}



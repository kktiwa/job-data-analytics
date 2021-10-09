case class Job(id: String,
               profile: JobProfile
              )

case class JobProfile(firstName: String,
                      lastName: String,
                      jobHistory: Option[Seq[JobHistory]] = None
                     )

case class JobHistory(title: String,
                      location: String,
                      salary: BigDecimal,
                      fromDate: String,
                      toDate: Option[String] = None
                     )

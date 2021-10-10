# Job Data Analytics

This project uses test data containing job history information for various candidate profiles and produces various
metrics using spark transformations.

### Assumptions

* Profiles with same first name and last name are different people
* Once a person creates their profile, they get a unique `id` and all job history is maintained in `jobHistory`
  attribute
* If `toDate` value is missing from a `jobHistory` row, it means that the person still works there
* For computing `most popular job title`, most frequently occurring job title for the year has been considered

### Tech Stack

* Scala
* Spark
* SBT

### Running the application

* Import the application as an SBT project in Intellij Idea
* Store the test data for the application somewhere on your local machine
* `JobMetricsAppSpec.scala` serves as a driver program (though a test) which allows running individual metric function
  and displaying the result
* Change the value of `inputDataPath` variable in `JobMetricsAppSpec.scala` to point to the local path where test data
  is stored
* Change the value of `writePath` variable in `JobMetricsAppSpec.scala` to point to path where the parquet file should
  be written to
* Run the `JobMetricsAppSpec.scala` test which runs each of the metric functions and outputs the results

### Solutions

Output results for various questions are written in `solution-answers.txt`

### Improvements

* Refactor `JobMetricsAppSpec.scala` to test on smaller spark datasets as unit tests
* Add a config framework and move spark related configurations and file paths etc. into it
* Add a spark submit script under `scripts` folder which will run the assembly JAR `job-data-analytics.jar` with
  appropriate spark configuration settings
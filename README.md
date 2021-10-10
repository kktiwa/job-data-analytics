# Job Data Analytics

This project uses test data containing job history information for various candidate profiles and produces various
metrics using spark.

### Assumptions

* If `toDate` value is missing from a `jobHistory` row, it means that the person still works there'

### Tech Stack

* Scala
* Spark

### Solutions

Output results for various questions are written in `solution-answers.txt`

### Improvements

* Unit tests using spark datasets
* Add a config framework
* Spark submit script
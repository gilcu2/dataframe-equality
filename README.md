# Dataframe equality

Given two DataFrames of unknown schema, how could you verify that their 
contents were exactly the same, given a Seq[String] containing the names 
of the columns which uniquely identify a row (i.e. primary key columns)


## Tests

* sbt test
* sbt acceptance:test

## Run

1. Install spark and add bin directory to PATH
1. sbt assembly
1. cd scripts
1. ./equality.sh 
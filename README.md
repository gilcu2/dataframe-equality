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
1. ./equality.sh [-k nKeys] [-o nOther] [sizes]

## Example

./equality.sh -o 3 200000 400000 800000

Results (ms):
Size                    200000  400000  800000

Equal:
Except                  4373    4850    12043
Hashcode                2614    4187    9188
Comparison              2264    3613    7227

Different:
Except                  4426    4748    10205
Hashcode                2383    3643    7007
Comparison              1943    3909    7175


#!/usr/bin/env bash

MASTER="local[8]"
#MASTER=spark://pop-os.localdomain:7077
CONFIGPATH="."
PROGRAM="../target/scala-2.11/DataFrameEquality.jar"
MAIN=com.gilcu2.DataFrameEqualityMain
OUT=equality.out
ERR=equality.err
if [[ $DEBUG ]];then
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
fi

spark-submit \
--class $MAIN \
--master $MASTER \
--driver-memory 12g \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
$PROGRAM "$@" 2>$ERR |tee $OUT

echo Output is in $OUT, error output in $ERR

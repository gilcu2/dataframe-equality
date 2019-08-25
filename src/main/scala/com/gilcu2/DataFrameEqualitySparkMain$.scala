package com.gilcu2

import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, SparkMainTrait}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object DataFrameEqualitySparkMain$ extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: CommandParameterValues)(
    implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val results = lineArguments.sizes.map(equality_experiment)

  }

  def getConfigValues(conf: Config): ConfigValuesTrait = {
    val firstPath = conf.getString("FirstDataFramePath")
    val secondPath = conf.getString("SecondDataFramePath")
    val keyFields = conf.getString("KeyFields").split(",")
    ConfigValues(firstPath, secondPath, keyFields)
  }

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val sizes = parsedArgs.sizes()

    CommandParameterValues(logCountsAndTimes, sizes)
  }

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val sizes = trailArg[List[Int]]()

  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, sizes: Seq[Int]) extends LineArgumentValuesTrait

  case class ConfigValues(firstPath:String, secondPath:String, keyFields:Array[String]) extends ConfigValuesTrait


}

package com.gilcu2

import com.gilcu2.experiments.Performance
import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, SparkMainTrait}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object DataFrameEqualityMain extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    val lineParameter = lineArguments.asInstanceOf[CommandParameterValues]

    val results = Performance.measureTimes(lineParameter.sizes,
      lineParameter.nKeys, lineParameter.nOtherFields)

  }

  def getConfigValues(conf: Config): ConfigValues = {
    val firstPath = conf.getString("FirstDataFramePath")
    val secondPath = conf.getString("SecondDataFramePath")
    val keyFields = conf.getString("KeyFields").split(",")
    ConfigValues(firstPath, secondPath, keyFields)
  }

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): CommandParameterValues = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val nKeys = parsedArgs.nKeys()
    val nOtherFields = parsedArgs.nOtherFields()
    val sizes = parsedArgs.sizes()

    CommandParameterValues(logCountsAndTimes, sizes, nKeys, nOtherFields)
  }

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val nKeys = opt[Int](default = Some(2))
    val nOtherFields = opt[Int](default = Some(2))
    val sizes = trailArg[List[Int]](default = Some(List(1000)))
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean,
                                    sizes: Seq[Int],
                                    nKeys: Int,
                                    nOtherFields: Int
                                   ) extends LineArgumentValuesTrait

  case class ConfigValues(firstPath: String, secondPath: String, keyFields: Array[String]) extends ConfigValuesTrait


}

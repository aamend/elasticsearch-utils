package com.aamend.elasticsearch

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.cli.{HelpFormatter, DefaultParser, Options, OptionBuilder}

/**
  * Created by antoine on 11/05/2016.
  */
object Utils extends LazyLogging {

  def main(args: Array[String]) = {

    val options = new Options()
    options.addOption("f", "output-file", true, "The output file to write bulk data to")
    options.addOption("i", "input-index", true, "The input index/type to read data")
    options.addOption("o", "output-index", true, "The output index/type to re-index data to")
    options.addOption("w", "write", false, "Write back to elasticsearch")
    options.addOption("h", "help", false, "Prints help menu")

    val parser = new DefaultParser()
    val cmd = parser.parse( options, args)

    if(cmd.hasOption("h")){
      val formatter = new HelpFormatter()
      formatter.printHelp("Utils", options)
      System.exit(1)
    }

    if(!cmd.hasOption("i")){
      logger.error("input index must be specified using -i parameter")
      System.exit(1)
    }

    if(!cmd.hasOption("o")){
      logger.error("output index must be specified using -o parameter")
      System.exit(1)
    }

    if(!cmd.hasOption("w")){
      if(!cmd.hasOption("f")){
        logger.error("output file must be specified using -f parameter if -w disabled")
        System.exit(1)
      }
    }

    val Array(inputIndexName, inputIndexType) = cmd.getOptionValue("i").split("/").take(2)
    val Array(outputIndexName, outputIndexType) = cmd.getOptionValue("o").split("/").take(2)

    val outputFile = {
      if(cmd.hasOption("f")) {
        Some(cmd.getOptionValue("f"))
      } else {
        None: Option[String]
      }
    }

    val write = cmd.hasOption("w")

    new ESUtils().process(inputIndexName, inputIndexType, write, outputIndexName, outputIndexType, outputFile)

  }


}

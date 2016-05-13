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
    options.addOption("i", "input-index", true, "The input indexName/indexType to read data")
    options.addOption("o", "output-index", true, "The output indexName/indexType to re-index data to")
    options.addOption("w", "write", false, "Write back to elasticsearch")
    options.addOption("h", "help", false, "Prints help menu")

    val parser = new DefaultParser()
    val cmd = parser.parse( options, args)

    if(args.isEmpty || cmd.hasOption("h")) usage(options)

    if(!cmd.hasOption("i")){
      logger.error("input index must be specified using -i parameter")
      usage(options)
    }

    if(cmd.hasOption("w")){
      if(!cmd.hasOption("o")){
        logger.error("output index must be specified when write option is enabled. Please use -o parameter")
        usage(options)
      }
    }

    if(!cmd.hasOption("w") && !cmd.hasOption("f")){
      logger.error("An output must be specified. Please enable either w (write) or f (file) or both")
      usage(options)
    }

    val Array(inputIndexName, inputIndexType) = cmd.getOptionValue("i").split("/").take(2)
    val Array(outputIndexName, outputIndexType) = {
      if(cmd.hasOption("o")){
        cmd.getOptionValue("o").split("/").take(2).map(s => Some(s))
      } else {
        Array(None: Option[String], None: Option[String])
      }
    }

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

  def usage(options: Options) = {
    val formatter = new HelpFormatter()
    formatter.printHelp("elasticsearch-utils", options)
    System.exit(1)
  }


}

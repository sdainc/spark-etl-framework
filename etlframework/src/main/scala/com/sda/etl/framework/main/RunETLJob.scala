package com.sda.etl.framework.main

import com.sda.etl.framework.common.{SparkSessionBuilder, Utils}
import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.types.FileLocation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object RunETLJob {

  val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {


    logger.info("Validate number of parameters")
    logger.info("Param 1::Environment:: " + args(0))
    logger.info("Param 2::Raw File Name:: " + args(1))

        if (args.length != 2) {
          val error = "Invalid No of Parameters. " +
                      "Expected Params - " +
                      "Param 1: <Environment - local / dev / prod>" +
                      "Param 2: <Raw File name OR Table name>"
          throw new RuntimeException(error)
        }
    
    logger.info("Parameters validated")
    logger.info("Load Environment Properties")

    val appConfig: AppConfig = new AppConfig(args(0), args(1), args(2))
    Utils.createPartitionKey(appConfig)

    logger.info("Creating SparkSession")

    val spark = SparkSessionBuilder.getSparkSession(appConfig)

    logger.info("Read Raw Data for: " + appConfig.rawFileName)

    // this landing folder is expected to be truncated through ADF
    // and latest data pulled here
    val tableDF: DataFrame = Utils.getLandingFileReader(appConfig).read(appConfig, spark, FileLocation.LANDING)


    logger.info("Raw data has been read for: " + appConfig.rawFileName)
    val rawDFWithAuditCols = Utils.createRawDFWithAuditCols(spark, appConfig, tableDF)

    logger.info("Process Data for: " + appConfig.rawFileName)

    val processedDataDF: DataFrame = Utils.getProcessor(appConfig).process(appConfig, spark, rawDFWithAuditCols)

    logger.info("Raw data has been processed for: " + appConfig.rawFileName)

    logger.info("Write Processed Data for: " + appConfig.rawFileName)

    Utils.getWriter(appConfig).write(appConfig, spark, processedDataDF, FileLocation.FINAL)

    logger.info("Processed data has been written for: " + appConfig.rawFileName)
  }

}

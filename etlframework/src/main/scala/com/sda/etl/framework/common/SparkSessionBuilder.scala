package com.sda.etl.framework.common

import com.sda.etl.framework.config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionBuilder {
  val logger                     = LoggerFactory.getLogger(this.getClass().getName);
  var sparkSession: SparkSession = null

  def getSparkSession(appConfig: AppConfig): SparkSession = {

    if (sparkSession != null) {
      return sparkSession
    } else {
      val sparkConf = new SparkConf()

      logger.info("Creating SparkSession")
      sparkSession = if (appConfig.env == "local") {
        SparkSession.
        builder().
        master("local").
        config(sparkConf).
        config("spark.sql.shuffle.partitions", "1").
        appName(appConfig.fileConfig.appName).
        getOrCreate()
      } else {
        SparkSession.
        builder().
        config(sparkConf).
        appName(appConfig.fileConfig.appName).
        getOrCreate()
      }
      logger.info("Created SparkSession")
      logger.info("Set Log Level")

      return sparkSession
    }
  }


}

package com.sda.etl.framework.etlservices

import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.types.FileLocation.FileLocation
import org.apache.spark.sql.{DataFrame, SparkSession}

trait PartitionReader {

  def read(appConfig: AppConfig, spark: SparkSession, fileLocation : FileLocation, partition:String): DataFrame

}
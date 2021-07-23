package com.sda.etl.framework.etlservices

import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.types.FileLocation.FileLocation
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer {

  def write(appConfig: AppConfig, spark: SparkSession, processedDataDF:DataFrame,fileLocation : FileLocation): Unit

}

package com.sda.etl.framework.etlservices

import com.sda.etl.framework.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Processor {

  def process(appConfig: AppConfig, spark: SparkSession, tableDF: DataFrame): DataFrame

}

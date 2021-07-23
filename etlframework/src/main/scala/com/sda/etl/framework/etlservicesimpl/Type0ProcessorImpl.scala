package com.sda.etl.framework.etlservicesimpl

import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.etlservices.Processor
import org.apache.spark.sql.{DataFrame, SparkSession}

class Type0ProcessorImpl extends Processor {

  def process(appConfig: AppConfig, spark: SparkSession, tableDF: DataFrame): DataFrame = {

    val processedDataDf: DataFrame = tableDF
    processedDataDf

  }


}

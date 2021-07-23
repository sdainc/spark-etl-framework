package com.sda.etl.framework.etlservicesimpl

import com.sda.etl.framework.common.Utils
import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.etlservices.Reader
import com.sda.etl.framework.types.FileLocation.FileLocation
import com.sda.etl.framework.types.ReadFileType
import org.apache.spark.sql.{DataFrame, SparkSession}

class LandingFileReaderImpl extends Reader {

  def read(appConfig: AppConfig, spark: SparkSession, fileLocation: FileLocation): DataFrame = {

    val dataFrame: DataFrame = appConfig.readFileType match {
      case ReadFileType.CSV =>
        spark.read.
        schema(appConfig.fileConfig.rawFileSchema).
        option("header", true).
        option("delimiter", appConfig.fileConfig.readDelim).
        csv(Utils.getFilePath(appConfig, fileLocation))
    }

    dataFrame

  }
}

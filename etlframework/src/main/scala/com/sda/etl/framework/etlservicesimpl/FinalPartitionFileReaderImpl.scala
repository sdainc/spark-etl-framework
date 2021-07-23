package com.sda.etl.framework.etlservicesimpl

import com.sda.etl.framework.common.Utils
import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.etlservices.PartitionReader
import com.sda.etl.framework.types.FileLocation.FileLocation
import com.sda.etl.framework.types.ReadFileType
import org.apache.spark.sql.{DataFrame, SparkSession}

class FinalPartitionFileReaderImpl extends PartitionReader {

  def read(appConfig: AppConfig, spark: SparkSession, fileLocation: FileLocation, partition:String): DataFrame = {

    val dataFrame: DataFrame = appConfig.readFileType match {
      case ReadFileType.CSV =>
        spark.read.
        schema(appConfig.fileConfig.finalFileSchema).
        option("header", false).
        option("delimiter", appConfig.fileConfig.readDelim).
        csv(Utils.getPartitionFilePath(appConfig, fileLocation,partition))
    }

    dataFrame

  }
}

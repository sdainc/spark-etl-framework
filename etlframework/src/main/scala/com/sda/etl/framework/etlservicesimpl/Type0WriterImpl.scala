package com.sda.etl.framework.etlservicesimpl

import com.sda.etl.framework.common.Utils
import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.etlservices.Writer
import com.sda.etl.framework.types.FileLocation.FileLocation
import com.sda.etl.framework.types.WriteFileType
import org.apache.spark.sql.{DataFrame, SparkSession}

class Type0WriterImpl extends Writer {

  def write(appConfig: AppConfig, spark: SparkSession, processedDataDF: DataFrame,fileLocation: FileLocation): Unit = {

    appConfig.writeFileType match {
      case WriteFileType.CSV => processedDataDF.write.
                               option("header", false).
                               mode(appConfig.fileConfig.saveMode.toLowerCase).
                               format("csv").
                               save(Utils.getFilePath(appConfig, fileLocation))
    }

  }


}

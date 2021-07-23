package com.sda.etl.framework.etlservicesimpl

import com.sda.etl.framework.common.Utils
import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.etlservices.Processor
import com.sda.etl.framework.types.FileLocation
import org.apache.spark.sql.{DataFrame, SparkSession}

class Type1ProcessorImpl extends Processor {

  def process(appConfig: AppConfig, spark: SparkSession, rawDFWithAuditCols: DataFrame): DataFrame = {

    val currentTableDF = Utils.getFinalFileReader(appConfig).read(appConfig, spark, FileLocation.FINAL_FULL).filter("CURRENT == 'Y' ")
    currentTableDF.cache()
    currentTableDF.createOrReplaceTempView(appConfig.fileConfig.currentTable)
    rawDFWithAuditCols.createOrReplaceTempView(appConfig.fileConfig.rawFileName)

    val finalSQL : String = appConfig.fileConfig.isPartitioned match {
      case "Y" =>
        s"""
           | SELECT
           | ${appConfig.fileConfig.rawFileName}.*,
           | 'NewRows' AS rowType,
           | ${appConfig.fileConfig.rawFileName}.partitionKey as pKey
           | FROM ${appConfig.fileConfig.rawFileName}
           | LEFT JOIN ${appConfig.fileConfig.currentTable}
           | ON ${appConfig.fileConfig.rawFileName}.${appConfig.fileConfig.rawFileMergeKey} =
           |    ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey}
           | WHERE ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey} IS NULL
           | UNION ALL
           | SELECT
           | ${appConfig.fileConfig.rawFileName}.*,
           | 'CurrentRows' AS rowType,
           | ${appConfig.fileConfig.currentTable}.partitionKey as pKey
           | FROM ${appConfig.fileConfig.rawFileName}
           | LEFT JOIN ${appConfig.fileConfig.currentTable}
           | ON ${appConfig.fileConfig.rawFileName}.${appConfig.fileConfig.rawFileMergeKey} =
           |    ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey}
           | WHERE ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey} IS NOT NULL
           | AND ${appConfig.fileConfig.rawFileName}.rowHash !=
           |     ${appConfig.fileConfig.currentTable}.rowHash
       """.stripMargin
      case "N" =>
        s"""
           | SELECT
           | ${appConfig.fileConfig.rawFileName}.*,
           | 'NewRows' AS rowType
           | FROM ${appConfig.fileConfig.rawFileName}
           | LEFT JOIN ${appConfig.fileConfig.currentTable}
           | ON ${appConfig.fileConfig.rawFileName}.${appConfig.fileConfig.rawFileMergeKey} =
           |    ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey}
           | WHERE ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey} IS NULL
           | UNION ALL
           | SELECT
           | ${appConfig.fileConfig.rawFileName}.*,
           | 'CurrentRows' AS rowType
           | FROM ${appConfig.fileConfig.rawFileName}
           | LEFT JOIN ${appConfig.fileConfig.currentTable}
           | ON ${appConfig.fileConfig.rawFileName}.${appConfig.fileConfig.rawFileMergeKey} =
           |    ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey}
           | WHERE ${appConfig.fileConfig.currentTable}.${appConfig.fileConfig.currentTableMergeKey} IS NOT NULL
           | AND ${appConfig.fileConfig.rawFileName}.rowHash !=
           |     ${appConfig.fileConfig.currentTable}.rowHash
       """.stripMargin
    }
    val finalDF = spark.sql(finalSQL)
    finalDF
  }


}

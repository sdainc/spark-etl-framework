package com.sda.etl.framework.etlservicesimpl

import com.sda.etl.framework.common.Utils
import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.etlservices.Writer
import com.sda.etl.framework.types.FileLocation.FileLocation
import com.sda.etl.framework.types.{FileLocation, WriteFileType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Type1WriterImpl extends Writer {

  def write(appConfig: AppConfig, spark: SparkSession, processedTableDF: DataFrame, fileLocation: FileLocation): Unit = {

    appConfig.fileConfig.isPartitioned match {
      case "N" => {
        val nonPartitionedTable = new NonPartitionedTable
        nonPartitionedTable.write(appConfig, spark, processedTableDF, fileLocation)
      }
      case "Y" => {
        val partitionedTable = new PartitionedTable
        partitionedTable.write(appConfig, spark, processedTableDF, fileLocation)
      }
    }


  }

  class PartitionedTable {
    def write(appConfig: AppConfig, spark: SparkSession, processedTableDF: DataFrame, fileLocation: FileLocation) = {

      processedTableDF.cache()
      processedTableDF.createOrReplaceTempView("processedTableDF")

      var currentRowsPartitionKeys = processedTableDF.select("partitionKey").distinct()

      val partitionReader = Utils.getFinalPartitionFileReader(appConfig)

      for (partitionKey <- currentRowsPartitionKeys.collect()) {

        val partition = partitionKey.getInt(0)
        val newRowsPartitionToProcessDF = processedTableDF.
                                          filter(s"partitionKey == $partition ").
                                          filter("rowType == 'NewRows' ").
                                          //withColumn("partitionKey", $"pKey").
                                          drop("pKey").
                                          drop("rowType")
        val existingRowsPartitionToProcessDF = processedTableDF.
                                               filter(s"partitionKey == $partition ").
                                               filter("rowType == 'CurrentRows' ").
                                               //     withColumn("partitionKey", $"pKey").
                                               drop("pKey").
                                               drop("rowType")
        var sqlOriginalPartition: String = ""
        try {
          val originalPartitionDF = partitionReader.
                                    read(appConfig,
                                          spark,
                                          FileLocation.FINAL_PARTITION,
                                          partition.toString)
          originalPartitionDF.cache().take(1)
          originalPartitionDF.createOrReplaceTempView("originalRowsDF")
          sqlOriginalPartition =
            s"""SELECT
               | originalRowsDF.*
               | FROM originalRowsDF
               | LEFT JOIN existingRowsPartitionToProcessDF
               | ON originalRowsDF.${appConfig.fileConfig.currentTableMergeKey} =
               |   existingRowsPartitionToProcessDF.${appConfig.fileConfig.currentTableMergeKey}
               | WHERE existingRowsPartitionToProcessDF.rowHash IS NULL
               | UNION ALL
               """.stripMargin
        }
        catch {
          case ex: Exception => println("Partition error")
        }
        newRowsPartitionToProcessDF.createOrReplaceTempView("newRowsPartitionToProcessDF")
        existingRowsPartitionToProcessDF.createOrReplaceTempView("existingRowsPartitionToProcessDF")
        val finalSQL = sqlOriginalPartition +
                       s"""
                          | SELECT
                          | newRowsPartitionToProcessDF.*
                          | FROM newRowsPartitionToProcessDF
                          | UNION ALL
                          | SELECT
                          | existingRowsPartitionToProcessDF.*
                          | FROM existingRowsPartitionToProcessDF
             """.stripMargin
        val finalDF = spark.sql(finalSQL).coalesce(1)
        finalDF.cache().take(1)
        appConfig.writeFileType match {
          case WriteFileType.CSV => finalDF.write.
                                    option("header", false).
                                    mode(appConfig.fileConfig.saveMode.toLowerCase).
                                    format("csv").
                                    save(Utils.getPartitionFilePath(appConfig, FileLocation.FINAL_PARTITION, partition.toString))
        }

      }

      currentRowsPartitionKeys = processedTableDF.filter("pKey != partitionkey").select("pKey").distinct()

      for (partitionKey <- currentRowsPartitionKeys.collect()) {

        val partition = partitionKey.getInt(0)
        val existingRowsPartitionToProcessDF = processedTableDF.
                                               filter(s"pKey == $partition ").
                                               filter("rowType == 'CurrentRows' ").
                                          //     withColumn("partitionKey", $"pKey").
                                               drop("pKey").
                                               drop("rowType")
        var sqlOriginalPartition: String = ""
        try {
          val originalPartitionDF = partitionReader.
                                    read(appConfig,
                                          spark,
                                          FileLocation.FINAL_PARTITION,
                                          partition.toString)
          originalPartitionDF.cache().take(1)
          originalPartitionDF.createOrReplaceTempView("originalRowsDF")
          existingRowsPartitionToProcessDF.createOrReplaceTempView("existingRowsPartitionToProcessDF")
          sqlOriginalPartition =
            s"""SELECT
               | originalRowsDF.*
               | FROM originalRowsDF
               | LEFT JOIN existingRowsPartitionToProcessDF
               | ON originalRowsDF.${appConfig.fileConfig.currentTableMergeKey} =
               |   existingRowsPartitionToProcessDF.${appConfig.fileConfig.currentTableMergeKey}
               | WHERE existingRowsPartitionToProcessDF.rowHash IS NULL
               """.stripMargin
        }
        catch {
          case ex: Exception => println("Partition error")
        }
        val finalSQL = sqlOriginalPartition
        val finalDF = spark.sql(finalSQL).coalesce(1)
        finalDF.cache().take(1)
        appConfig.writeFileType match {
          case WriteFileType.CSV => finalDF.write.
                                    option("header", false).
                                    mode(appConfig.fileConfig.saveMode.toLowerCase).
                                    format("csv").
                                    save(Utils.getPartitionFilePath(appConfig, FileLocation.FINAL_PARTITION, partition.toString))
        }

      }
    }
  }

  class NonPartitionedTable {
    def write(appConfig: AppConfig, spark: SparkSession, processedTableDF: DataFrame,
              fileLocation: FileLocation): Unit = {

      processedTableDF.cache()

      val newRowsDF = processedTableDF.
                      filter("rowType == 'NewRows' ").
                      drop("rowType").
                      drop("pKey")
      newRowsDF.createOrReplaceTempView("newRowsDF")

      val existingRowsDF = processedTableDF.
                           filter("rowType == 'CurrentRows' ").
                           drop("rowType").
                           drop("pKey")
      existingRowsDF.createOrReplaceTempView("changedRowsDF")

      val originalFileDF = Utils.getFinalFileReader(appConfig).
                           read(appConfig, spark, FileLocation.FINAL)
      originalFileDF.cache()
      originalFileDF.createOrReplaceTempView("originalRowsDF")

      val finalRowsSQL =
        s"""
           | SELECT
           | originalRowsDF.*
           | FROM originalRowsDF
           | LEFT JOIN changedRowsDF
           | ON originalRowsDF.${appConfig.fileConfig.currentTableMergeKey} =
           |    changedRowsDF.${appConfig.fileConfig.currentTableMergeKey}
           | WHERE changedRowsDF.rowHash IS NULL
           | UNION ALL
           | SELECT
           | newRowsDF.*
           | FROM newRowsDF
           | UNION ALL
           | SELECT
           | changedRowsDF.*
           | FROM changedRowsDF
           """.stripMargin

      val finalDF = spark.sql(finalRowsSQL).coalesce(1)
      finalDF.take(1)

      appConfig.writeFileType match {
        case WriteFileType.CSV => finalDF.write.
                                  option("header", false).
                                  mode(appConfig.fileConfig.saveMode.toLowerCase).
                                  format("csv").
                                  save(Utils.getFilePath(appConfig, fileLocation))
      }
    }
  }

}
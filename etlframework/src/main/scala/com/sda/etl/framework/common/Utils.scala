package com.sda.etl.framework.common

import java.util.Calendar

import com.sda.etl.framework.config.AppConfig
import com.sda.etl.framework.types.FileLocation.FileLocation
import com.sda.etl.framework.etlservices.{PartitionReader, Processor, Reader, Writer}
import com.sda.etl.framework.etlservicesimpl._
import com.sda.etl.framework.types.{FileLocation, SCDType}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {

  val logger = Logger.getLogger(this.getClass.getName)

  def getLandingFileReader(appConfig: AppConfig): Reader = {

    val reader: Reader = appConfig.fileConfig.sourceType match {
      case "file" => new LandingFileReaderImpl
    }

    reader
  }

  def getFinalFileReader(appConfig: AppConfig): Reader = {

    val reader: Reader = appConfig.fileConfig.sourceType match {
      case "file" => new FinalFileReaderImpl
    }

    reader
  }

  def getFinalPartitionFileReader(appConfig: AppConfig): PartitionReader = {

    val reader: PartitionReader = appConfig.fileConfig.sourceType match {
      case "file" => new FinalPartitionFileReaderImpl
    }

    reader
  }

  def getProcessor(appConfig: AppConfig): Processor = {

    val processor: Processor = appConfig.scdType match {
      case SCDType.TYPE0 => new Type0ProcessorImpl
      case SCDType.TYPE1 => new Type1ProcessorImpl
      case SCDType.TYPE2 => new Type2ProcessorImpl
    }

    processor
  }

  def getWriter(appConfig: AppConfig): Writer = {

    val writer: Writer = appConfig.scdType match {
      case SCDType.TYPE0 => new Type0WriterImpl
      case SCDType.TYPE1 => new Type1WriterImpl
      case SCDType.TYPE2 => new Type2WriterImpl
    }

    writer
  }

  def getFilePath(appConfig: AppConfig, fileLocation: FileLocation): String = {

    val partition = appConfig.fileConfig.isPartitioned match {
      case "Y" => "/partitionkey=" + appConfig.partitionKey
      case "N" => ""
    }
    val filePath: String = fileLocation match {
      case FileLocation.LANDING    => appConfig.rawBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/" + appConfig.fileConfig.rawFilePattern
      case FileLocation.TEMP       => appConfig.processBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/temp"
      case FileLocation.FINAL      => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + partition
      case FileLocation.FINAL_FULL => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir
    }
    filePath
  }

  def getPartitionFilePath(appConfig: AppConfig, fileLocation: FileLocation, pKey: String): String = {

    val partition = "/partitionkey=" + pKey

    val filePath: String = fileLocation match {
      case FileLocation.LANDING         => appConfig.rawBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/" + appConfig.fileConfig.rawFilePattern
      case FileLocation.TEMP            => appConfig.processBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/temp"
      case FileLocation.FINAL           => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + partition
      case FileLocation.FINAL_FULL      => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir
      case FileLocation.FINAL_PARTITION => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + partition
    }
    filePath
  }

  def createRawDFWithAuditCols(spark: SparkSession, appConfig: AppConfig,
                               tableDF: DataFrame)
  : DataFrame = {

    logger.info("createHashColString")
    createHashColString(appConfig)
    appConfig.fileConfig.isPartitioned match {
      case "Y" =>
        logger.info("createPartitionKey")
        createPartitionKey(appConfig)
      case "N" => logger.info("No partition key required")
    }
    logger.info("createRawFileSQL")
    createRawFileSQL(appConfig)
    tableDF.createOrReplaceTempView(appConfig.rawFileName)
    logger.info("Final Raw SQL: " + appConfig.finalRawFileSQL)
    spark.sqlContext.udf.register("sha256Hash",
                                          (text: String) =>
                                          String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8")))))
    val rawDFWithAuditCols: DataFrame = spark.sql(appConfig.finalRawFileSQL)
    rawDFWithAuditCols

  }

  def createHashColString(appConfig: AppConfig): Unit = {

    val colArray: Array[String] = appConfig.fileConfig.sqlSelect.split(",")
    val hashColArray: Array[String] = colArray.map(col => "if(isNull(" + col +
                                                          "),'',cast(" + col +
                                                          " as string )) ")
    appConfig.hashCols = hashColArray.mkString(",")
    appConfig.sqlAuditCols = appConfig.sqlAuditCols.replace("cols", appConfig.hashCols)
  }

  def createSQLType2String(appConfig: AppConfig): String = {

    val sqlColArray: Array[String] = appConfig.fileConfig.sqlSelect.split(",")
    val sqlReplacedArray: Array[String] = sqlColArray.map(col => "originalRowsDF." + col.trim)
    val sqlReplaced = sqlReplacedArray.mkString(",")

    val sqlAuditColArray: Array[String] = appConfig.fileConfig.sqlAudit.split(",")
    val sqlReplacedAuditColArray: Array[String] = sqlAuditColArray.map(col => "originalRowsDF." + col.trim)
    val sqlReplacedAuditCol = sqlReplacedAuditColArray.mkString(",")

    var finalSQL = sqlReplaced + " , " + sqlReplacedAuditCol
    finalSQL = " " +
               finalSQL.
                  replace("originalRowsDF.current", " 'N' as current ").
                  replace("originalRowsDF.enddate", " cast(date_sub(current_timestamp(), 1) as string) enddate ") +
               " "

    finalSQL

  }

  def createPartitionKey(appConfig: AppConfig): Unit = {

    val now = Calendar.getInstance()
    val year = now.get(Calendar.YEAR)
    val month = "%02d".format(now.get(Calendar.MONTH) + 1)
    val date = "%02d".format(now.get(Calendar.DATE))
    val partitionKey = appConfig.pKey // year + month + date

    appConfig.partitionKey = partitionKey.toInt

    appConfig.sqlAuditCols = appConfig.sqlAuditCols.replace("partition_key", partitionKey)

  }

  def createRawFileSQL(appConfig: AppConfig): Unit = {

    appConfig.rawFileSQLSelect = " SELECT " + appConfig.fileConfig.sqlSelect + "," + appConfig.sqlAuditCols
    appConfig.rawFileSQLFrom = " FROM " + appConfig.rawFileName
    appConfig.finalRawFileSQL = appConfig.rawFileSQLSelect + appConfig.rawFileSQLFrom
  }


  /*
    def getPartitionFilePath(appConfig: AppConfig, operationType: OperationType): String = {

      val filePath: String = operationType match {
        // No partitionkey required
        case OperationType.READ_LANDING => appConfig.rawBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/" + appConfig.fileConfig.rawFilePattern
        // No partitionkey required
        case OperationType.READ_INTERMEDIATE => appConfig.processBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/temp"
        // No partitionkey required, its a full table read
        case OperationType.READ_FINAL => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir
        // partitionkey required if table is partitioned
        case OperationType.WRITE_FINAL => appConfig.loadBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir
        // No partitionkey required
        case OperationType.WRITE_INTERMEDIATE => appConfig.processBaseDir + appConfig.fileConfig.appDir + "/" + appConfig.fileConfig.tableDir + "/temp"
      }
      filePath
    }

    def getHeader(appConfig: AppConfig, operationType: OperationType): String = {

      val header: String = operationType match {
        // Raw files have headers
        case OperationType.READ_LANDING => "true"
        //While reading the files from ADLS final directory, header not required
        case OperationType.READ_FINAL => "false"
        //While reading the files from ADLS intermediate directory, header not required
        case OperationType.READ_INTERMEDIATE => "false"
        //While writing final ADLS directory, header not required
        case OperationType.WRITE_INTERMEDIATE => "false"
        //While writing final ADLS directory, header not required
        case OperationType.WRITE_FINAL => "false"
      }

      header
    }

    def getFileSchema(appConfig: AppConfig, operationType: OperationType): String = {

      val fileSchema: String = operationType match {
        // Raw files have different schema than final files written to ADLS
        case OperationType.READ_LANDING => appConfig.fileConfig.rawFileSchema
        // Final ADLS files have additional audit columns
        case OperationType.READ_FINAL => appConfig.fileConfig.finalFileSchema
        // Intermediate ADLS files also have audit columns
        case OperationType.READ_INTERMEDIATE => appConfig.fileConfig.finalFileSchema

      }
      fileSchema

    }

    def getPartitionKey(appConfig: AppConfig, operationType: OperationType): String = {

      val pKey: String = appConfig.partitionKey.toString
      pKey

    }


    def createPartitionKey(appConfig: AppConfig): Unit = {

      val now = Calendar.getInstance()
      val year = now.get(Calendar.YEAR)
      val month = "%02d".format(now.get(Calendar.MONTH) + 1)
      val date = "%02d".format(now.get(Calendar.DATE))
      val partitionKey = appConfig.pKey // year + month + date

      appConfig.partitionKey = partitionKey.toInt

      appConfig.sqlAuditCols = appConfig.sqlAuditCols.replace("partition_key", partitionKey)

    }



  */
}


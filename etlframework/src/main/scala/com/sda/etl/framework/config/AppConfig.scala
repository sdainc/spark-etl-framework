package com.sda.etl.framework.config

import com.sda.etl.framework.types.ReadFileType.ReadFileType
import com.sda.etl.framework.types.SCDType.SCDType
import com.sda.etl.framework.types.WriteFileType.WriteFileType
import com.sda.etl.framework.common.LoadFileConfig
import com.sda.etl.framework.types.{ReadFileType, SCDType, WriteFileType}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

class AppConfig(envParm: String, rawFileNameParm: String, partKey: String) {

  //remove partkey parameter in prod

  val logger = Logger.getLogger(this.getClass.getName)

  logger.info("Loading Application Configuration")

  val env                              = envParm
  val rawFileName                      = rawFileNameParm
  val conf                             = ConfigFactory.load
  val props                            = conf.getConfig(env)
  val rawBaseDir                       = props.getString("raw.base.dir")
  val processBaseDir                   = props.getString("process.base.dir")
  val loadBaseDir                      = props.getString("load.base.dir")
  val rawFileNameJson                  = rawFileName + ".json"
  val fileConfig       : FileConfig    = LoadFileConfig.getFileConfig(rawFileNameJson)
  val readFileType     : ReadFileType  = fileConfig.inFileType.toLowerCase
  match {
    case "csv"     => ReadFileType.CSV
    case "txt"     => ReadFileType.TXT
    case "parquet" => ReadFileType.PARQUET
    case "orc"     => ReadFileType.ORC
    case "delta"   => ReadFileType.DELTA
  }
  val writeFileType    : WriteFileType = fileConfig.outFileType.toLowerCase
  match {
    case "csv"     => WriteFileType.CSV
    case "txt"     => WriteFileType.TXT
    case "parquet" => WriteFileType.PARQUET
    case "orc"     => WriteFileType.ORC
    case "delta"   => WriteFileType.DELTA
  }
  val scdType          : SCDType       = fileConfig.scdType.toLowerCase
  match {
    case "type1" => SCDType.TYPE1
    case "type2" => SCDType.TYPE2
    case "type0" => SCDType.TYPE0
  }
  var readOperationType: String        = null
  var sqlAuditCols     : String        = null
  fileConfig.isPartitioned match {
    case "Y" =>
      sqlAuditCols =
        s"""
           | 'Y'                    as CURRENT,
           | current_date           as EFFECTIVEDATE,
           | ''                     as ENDDATE,
           | sha2(concat(cols),256) as ROWHASH,
           | partition_key          as PARTITIONKEY
     """.stripMargin
    case "N" =>
      sqlAuditCols =
        s"""
           | 'Y'                    as CURRENT,
           | current_date           as EFFECTIVEDATE,
           | ''                     as ENDDATE,
           | sha2(concat(cols),256) as ROWHASH
     """.stripMargin
  }
  var finalRawFileSQL     : String = null
  var hashCols            : String = null
  var rawFileSQLSelect    : String = null
  var rawFileSQLFrom      : String = null
  var partitionKey        : Int    = 0
  var currentFileSQLSelect: String = null
  var currentFileSQLFrom  : String = null
  var currentFileSQL      : String = null
  var pKey                : String = partKey
  logger.info("Application Configuration Loaded")

}

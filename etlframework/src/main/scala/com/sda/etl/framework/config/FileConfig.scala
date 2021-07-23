package com.sda.etl.framework.config

case class FileConfig(
                       appName: String,
                       sourceType: String,
                       rawFileName: String,
                       rawFilePattern: String,
                       appDir: String,
                       tableDir: String,
                       inFileType: String,
                       readDelim: String,
                       writeDelim: String,
                       saveMode: String,
                       scdType: String,
                       outFileType: String,
                       rawFileSchema: String,
                       sqlSelect: String,
                       sqlAudit: String,
                       currentTable: String,
                       finalFileSchema: String,
                       currentTableMergeKey: String,
                       rawFileMergeKey: String,
                       isPartitioned: String
                     )

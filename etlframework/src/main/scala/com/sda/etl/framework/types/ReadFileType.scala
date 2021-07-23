package com.sda.etl.framework.types

object ReadFileType extends Enumeration {
  type ReadFileType = Value
  val CSV, TXT, PARQUET, ORC, DELTA = Value
}

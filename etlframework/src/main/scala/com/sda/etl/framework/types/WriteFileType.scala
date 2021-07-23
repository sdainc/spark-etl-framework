package com.sda.etl.framework.types

object WriteFileType extends Enumeration {
  type WriteFileType = Value
  val CSV, TXT, PARQUET, ORC, DELTA = Value
}

package com.sda.etl.framework.types

object FileLocation extends Enumeration {
  type FileLocation = Value
  val LANDING, TEMP, FINAL, FINAL_FULL, FINAL_PARTITION = Value
}

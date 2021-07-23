package com.sda.etl.framework.common

import com.sda.etl.framework.config.FileConfig
import net.liftweb.json._

object LoadFileConfig {

  def getFileConfig(rawFileNameJson: String): FileConfig = {

    implicit val formats = DefaultFormats
    val json_content: String  = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(rawFileNameJson)).mkString
    val fileConfigJValue = parse(json_content)
    val fileConfig: FileConfig = fileConfigJValue.extract[FileConfig]
    fileConfig

  }
}

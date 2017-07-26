package com.wisecloud.wiselyzer.utils

import java.util.{List => JList, Map => JMap}

import collection.JavaConversions._
import com.fasterxml.jackson.annotation.JsonProperty
import jersey.repackaged.com.google.common.base.Preconditions
/**
  * Created by hanlinux on 12/07/2017.
  */
class WisecloudYaml(@JsonProperty("parameters") _parameters: JMap[String, String]) {
    
    val errMsg: String = "parameters cannot be null"
    val parameters: Map[String, String] = Preconditions.checkNotNull(_parameters, errMsg, null).toMap
    
}

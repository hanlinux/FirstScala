package com.wisecloud.wiselyzer.parse.test

/**
  * Created by hanlinux on 06/07/2017.
  */
object scalaTest extends App {
    val stationId = "FXGL_C03-5FT-02B_1_AE-22" //=> factoryCode, floor, line_id, machine_id
    
    val it = stationId.split("_").iterator
    
    println(getItValue(it))
    println(getItValue(it))
    println(getItValue(it))
    println(getItValue(it))
    println(getItValue(it))
    println(getItValue(it))
    
    def getItValue(it: Iterator[String]): String ={
        if(it.hasNext)
            it.next()
        else
            ""
    }
}

package com.wisecloud.wiselyzer.parse.driver

import java.sql.Date

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.joda.time.format.DateTimeFormat

/**
  * Created by hanlinux on 05/07/2017.
  */
object ExportDriver extends App {
    val sparkSession = SparkSession
        .builder() // important - call this rather than SparkSession.builder()
        .master("local") // change to "yarn-client" on YARN
        .config("spark.driver.cores", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "4g")
        .config("spark.cassandra.connection.host", "localhost")
        .config("spark.cassandra.connection.port", "9042")
        .appName("PDCA Parser")
        .getOrCreate();
    
    val sc = sparkSession.sparkContext
    val conn = CassandraConnector(sc.getConf)
    
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    
    val dataType = "pdca"
    val model = "rd" //EA, RD, ...
    val keyspace = "lx"
    val tableName = dataType + "_" + model
    val station = "DP2"
    val outputPath = keyspace + "_" + tableName + "_" + station
    
    val startTime = format.parseDateTime("2017-01-11 09:39:18")
    val endTime = format.parseDateTime("2017-01-11 09:39:18")
    
    exportByStation(startTime.toDate.getTime, endTime.toDate.getTime, keyspace, tableName, station, outputPath)
    
    def exportByStation(startTime: Long, endTime: Long, keyspace: String, tableName: String, station: String, outputPath: String): Unit = {
        val pdcaDf = sparkSession
            .read
            .cassandraFormat(tableName, keyspace)
            .load
            
        val queryResult = pdcaDf
            .select("*")
                .filter(row => {
                    row.getAs[String]("station_name").equals(station) &&
                    row.getAs[Date]("starttime").getTime >= startTime &&
                    row.getAs[Date]("starttime").getTime <= endTime
            })
    
        println(pdcaDf.columns.mkString(","))
        queryResult.rdd.take(10).foreach(println(_))
    
        queryResult.rdd.saveAsTextFile(outputPath)
    }
}

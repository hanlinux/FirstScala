package com.wisecloud.wiselyzer.parse.test

import com.datastax.spark.connector.cql.CassandraConnector
import com.wisecloud.wiselyzer.parse.driver.ImportDriver.createSchema
import com.wisecloud.wiselyzer.parse.pdca.PdcaParser
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

/**
  * Created by hanlinux on 29/06/2017.
  */
object PdcaParserTest extends App {
    
    val versionFilePath = "./sample_data/PDCA_With_Version_Col.Sample"
    val noVersionFilePath = "./sample_data/PDCA_Without_Version_Col.Sample"
    
    val sparkSession = SparkSession
        .builder() // important - call this rather than SparkSession.builder()
        .master("local") // change to "yarn-client" on YARN
        .config("spark.driver.cores", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "4g")
        .config("spark.cassandra.connection.host", "127.0.0.1")
        .config("spark.cassandra.connection.port", "9042")
        .appName("PDCA Parser Test")
        .getOrCreate();
    
    val sc = sparkSession.sparkContext
    val conn = CassandraConnector(sc.getConf)
    
    val docRootPath = "./"
    
    val dataType = "pdca"
    val model = "RD" //EA, RD, ...
    val keyspace = "lx"
    val tableName = dataType + "_" + model
    
    val replicationFactor = 1;
    
    createSchema(conn, replicationFactor.toString, docRootPath + File.separator + "schema", dataType, keyspace, tableName);
    
    val fileList = List(versionFilePath, noVersionFilePath)
    
    fileList.map(file => {
        new PdcaParser().parse(conn, keyspace, tableName, file)
    })
}
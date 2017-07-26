package com.wisecloud.wiselyzer.parse.driver

import java.io.FileReader

import com.datastax.spark.connector.cql._
import com.wisecloud.wiselyzer.parse.pdca.PdcaParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.wisecloud.wiselyzer.utils.{WisecloudYaml}

import scala.io.Source
import scala.reflect.io.File
import org.apache.spark.SparkContext
import java.util.Calendar
import java.text.SimpleDateFormat

/**
  * Created by hanlinux on 28/06/2017.
  */
object ImportDriver {
  
  def main(args: Array[String]): Unit = {
    
//      val wisecloudYamlPath = {
//          if (args.length == 0 || args(0).isEmpty)
//              new Path(new java.io.File(".").getCanonicalPath, "conf" + File.separator + "parser.yaml").toString
//          else
//              args(0).trim
//      }
    parseArgs(args)
    
    @transient
    var sparkSessionBuild = SparkSession
        .builder() // important - call this rather than SparkSession.builder()
//            .master("local") // change to "yarn-client" on YARN
//            .config("spark.driver.cores", "1")
//            .config("spark.driver.memory", "1g")
//            .config("spark.executor.cores", "2")
//            .config("spark.executor.memory", "4g")
//            .config("spark.cassandra.connection.host", "localhost")
//            .config("spark.cassandra.connection.port", "9042")
//            .appName("PDCA Parser")
//            .getOrCreate();

    wisecloudYaml.parameters
        .filterKeys(key => key.startsWith("spark"))
        .foreach(para => {
            println(s"${para._1} + ", s" + ${para._2}")
            sparkSessionBuild.config(para._1, para._2)
        })

    val sparkSession = sparkSessionBuild.getOrCreate()
    
    @transient
    val sc = sparkSession.sparkContext

    @transient
    val conn = CassandraConnector(sc.getConf)
    
    //Prepare Table
    //dataType = "pdca"
    //modelName = "RD" //EA, RD, ...
    val phase = this.phaseName
    val keyspace = this.keySpace
    val datatype = this.dataType
    val model = this.modelName
    val tableName = datatype + "_" + model
    val replicationFactor = this.replicationFactor
    val schemaPath = this.schemaRootPath
    val backupRootPath = this.dataBackupRootPath

    createSchema(conn, replicationFactor, schemaPath, datatype, keyspace, tableName);
    
    //Parsing Data
    val hadoopConf = new Configuration()
    val fileSystem = FileSystem.get(hadoopConf)
    val fileStatus = fileSystem.listStatus(new Path(dataInputRootPath))
    
    val filePathStringRdd = sc.parallelize(
          fileStatus.map(status => (status.getPath.toUri.getPath, status.getPath.getName))
        )//end of parallelize
    
    val backupDestPathPostfix: String = (s"$phase/$datatype/$model").toUpperCase
    val backupDestPath: String = s"$backupRootPath/$backupDestPathPostfix"
        
    val importResultRDD = filePathStringRdd.map(fileInfo => {
      val filePath = fileInfo._1
      val fileName = fileInfo._2
      
      println(s"==== processing $filePath ====")
      val pdcaParser = new PdcaParser();      
      val parseResult = pdcaParser.parse(conn, keyspace, tableName, filePath)
      
      println(s"==== Move file from filePath to $backupDestPath/$fileName ====")
      val hadoopConf = new Configuration()
      val fileSystem = FileSystem.get(hadoopConf)
      if (!fileSystem.exists(new Path(backupDestPath)))
        fileSystem.mkdirs(new Path(backupDestPath))
      val isMoved = fileSystem.rename(new Path(filePath), new Path(s"$backupDestPath/$fileName"))
      println(s"==== Move file from $filePath to $backupDestPath/$fileName was "+
          s"${isMoved match { case true => "succeed" case false => "not succeed" }} ====")
     
      //moveProcessedFileToBackupFolder(filePath, backupDestPath, fileName)
      ((fileName, filePath, getNowTimeString("yyyy-MM-dd HH-mm-ss"),
        s"message: ${parseResult._2}"), 
        parseResult._1 match {
          case true  => 1
          case false => 0
        }
      )
    })
    
    val successCnt = importResultRDD.values.sum.toInt
    val totalCnt   = importResultRDD.count.toInt
    val failureCnt = totalCnt - successCnt
    val date = getNowTimeString("yyyy-MM-dd")
    val time = getNowTimeString("HH-mm-ss")
    importResultRDD.coalesce(1).saveAsTextFile(s"$importedFilePath/$date/$time.txt")
    println(s"==== Total imported file count: $totalCnt, success count: $successCnt, failure count: $failureCnt ====")
  }
  
  def createSchema(conn: CassandraConnector, replicationFactor: String, schemaRootPath: String, dataType: String, keyspace : String, tableName : String): Unit ={
    val keyspaceSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_keyspace.schema").mkString.replace("{replicationFactor}", replicationFactor)
    
    val tableSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_" + dataType + "_table.schema").mkString.replace("{keyspace}", keyspace).replace("{tableName}", tableName)
    val indexSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_" + dataType + "_table_index.schema").getLines().map(line => line.replace("{keyspace}", keyspace).replace("{tableName}", tableName))

    val metaTableSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_" + dataType + "_meta_table.schema").mkString.replace("{keyspace}", keyspace).replace("{tableName}", "meta_" + tableName)
    val metaIndexSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_" + dataType + "_meta_table_index.schema").getLines().map(line => line.replace("{keyspace}", keyspace).replace("{tableName}", "meta_" + tableName))

    val specTableSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_" + dataType + "_spec_table.schema").mkString.replace("{keyspace}", keyspace).replace("{tableName}", "spec_" + tableName)
    val specIndexSchema = Source.fromFile(schemaRootPath + File.separator + "1212_" + keyspace + "_" + dataType + "_spec_table_index.schema").getLines().map(line => line.replace("{keyspace}", keyspace).replace("{tableName}", "spec_" + tableName))

    //Create Schema
    conn.withSessionDo { session =>
        session.execute(keyspaceSchema)
        
        session.execute(tableSchema)
        indexSchema.foreach(indexLine => session.execute(indexLine))

        session.execute(metaTableSchema)
        metaIndexSchema.foreach(indexLine => session.execute(indexLine))

        session.execute(specTableSchema)
        specIndexSchema.foreach(indexLine => session.execute(indexLine))
    }
  }
  
//  def moveProcessedFileToBackupFolder(orig: String, dest: String, fileName: String): Unit = {
//    //val postfix: String = (s"$phaseName/$dataType/$modelName").toUpperCase
//    //val postfix: String = (s"$dataType/$modelName").toUpperCase
//    //val dest = s"$dataBackupRootPath/$postfix"
//    println(s"Move file from $orig to $dest/$fileName")
//    val hadoopConf = new Configuration()
//    val fileSystem = FileSystem.get(hadoopConf)
//    fileSystem.mkdirs(new Path(dest))
//    val isMoved = fileSystem.rename(new Path(orig), new Path(s"$dest/$fileName"))
//    println(s"Move file from $orig to $dest/$fileName was "+
//        s"${isMoved match { case true => "succeed" case false => "not succeed" }} + ")
//  }
  
  def getNowTimeString(format: String): String = {
    val now = Calendar.getInstance()
    var formatString = format
    if (formatString == "")
      formatString = "yyy-MM-dd hh:mm:ss.SSS"
    var ff = new SimpleDateFormat(formatString)
    val nowDate = now.getTime()
    ff.format(nowDate)
  }
  
  def parseArgs(args: Array[String]) = {
    val argList = args.sliding(2, 1).toList
    argList.collect {
      case Array("--parserConf", config: String)        => {
        println(s" ==== $config ====")
        val reader = new FileReader(config)
        val mapper = new ObjectMapper(new YAMLFactory())
        wisecloudYaml = mapper.readValue(reader, classOf[WisecloudYaml])
        
        replicationFactor = wisecloudYaml.parameters("cassandra.replication.factor")
        schemaRootPath = wisecloudYaml.parameters("parser.cassandra.schema.root.path")
        dataInputRootPath = wisecloudYaml.parameters("parser.data.input.root.path")
        dataBackupRootPath = wisecloudYaml.parameters("parser.data.backup.root.path")        
        importedFilePath = wisecloudYaml.parameters("parser.data.processed.root.path")
      }
      case Array("--zone", zone: String)                => zoneName = zone
      case Array("--phase", phase: String)              => phaseName = phase
      case Array("--model", model: String)              => modelName = model
      case Array("--datatype", datatype: String)        => dataType = datatype
      case Array("--keyspace", keyspace: String)        => keySpace = keyspace
      case Array("--inputRootPath", inputDir: String)   => dataInputRootPath = inputDir
      case Array("--backupRootPath", backupDir: String)   => dataBackupRootPath = backupDir
      case Array("--schemaRootPath", schemaDir: String) => schemaRootPath = schemaDir
      case Array("--importedFilePath", importedPath: String) => importedFilePath = importedPath
      case Array("--debug", debug: String)              => debugMode =
        debug.toLowerCase() match {
          case "t" | "true"  => true
          case "f" | "false" => false
          case _             => false
        }
      }
  }
  
  //private var sc: SparkContext = _
  //var fileSystem: FileSystem = _
  var wisecloudYaml: WisecloudYaml = _
  var replicationFactor: String = "1"
  var dataBackupRootPath: String = "/tmp"
  var importedFilePath: String = "/backup/imported"
  var zoneName: String = "NONE"
  var modelName: String = "NONE"
  var dataInputRootPath: String = "/data"
  var schemaRootPath: String = "schema"
  var debugMode: Boolean = false
  var dataType: String = "NONE"
  var keySpace: String = "lx"
  var phaseName: String = "LX"
}

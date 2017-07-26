package com.wisecloud.wiselyzer.parse.pdca

import java.util.zip.ZipInputStream

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory, CompressionInputStream}

import scala.io.Source

/**
  * Created by hanlinux on 29/06/2017.
  */
abstract class BaseParser {
    
    protected var conn: CassandraConnector = null
    protected var keyspace: String = null
    protected var tableName: String = null
    protected var filePathString = ""
    protected var lineCount = 0
    protected var parserMessage: String = ""
    
    def parse(conn: CassandraConnector, keyspace: String, tableName: String, filePathString: String): (Boolean, String) = {

        this.conn = conn
        this.keyspace = keyspace
        this.tableName = tableName
        
        this.filePathString = filePathString
        this.lineCount = 0
        
        try{
          val hadoopConf = new Configuration()
          val fs = FileSystem.get(hadoopConf)
          val inputPath = new Path(filePathString)
      
          val factory = new CompressionCodecFactory(hadoopConf)
          val codec = factory.getCodec(inputPath)
          
          if (codec == null) {
              inputPath.getName match {
                  case f if(f.endsWith(".zip"))  => readZipConpressionFile(fs, inputPath, parseRow)
                  case _ => readNormalFile(fs, inputPath, parseRow)
              }
          } else {
              readHadoopSupportCompressionFile(fs, inputPath, codec, parseRow)
          }
        }catch{
          case ex: Exception => parserMessage = ex.getMessage
        }
        (parserMessage.isEmpty, parserMessage)
    }
    
    def readNormalFile(fs: FileSystem, inputPath: Path, parseFn: (String, String) => Unit): Unit ={
        //println("readNormalFile => " + inputPath.getName)
        val fsin = fs.open(inputPath)
        Source.fromInputStream(fsin).getLines().foreach(line => parseFn(line, ","))
    }
    
    def readZipConpressionFile(fs: FileSystem, inputPath: Path, parseFn: (String, String) => Unit): Unit ={
        //println("readZipConpressionFile => " + inputPath.getName)
        val fsin = fs.open(inputPath)
        val zfsin = new ZipInputStream(fsin)
        zfsin.getNextEntry
    
        Source.fromInputStream(zfsin).getLines().foreach(line => parseFn(line, ","))
    }
    
//    bzip2           .bz2            org.apache.hadoop.io.compress.BZip2Codec
//    default         .deflate        org.apache.hadoop.io.compress.DefaultCodec
//    deflate         .deflate        org.apache.hadoop.io.compress.DeflateCodec
//    gzip            .gz             org.apache.hadoop.io.compress.GzipCodec
//    lz4             .lz4            org.apache.hadoop.io.compress.Lz4Codec
//    snappy          .snappy         org.apache.hadoop.io.compress.SnappyCodec
    def readHadoopSupportCompressionFile(fs: FileSystem, inputPath: Path, codec: CompressionCodec, parseFn: (String, String) => Unit): Unit ={
        //println("readCompressionFile => " + inputPath.getName)
        var in: CompressionInputStream = null
        try {
            in = codec.createInputStream(fs.open(inputPath))

            Source.fromInputStream(in).getLines().foreach(line => parseFn(line, ","))
        } finally {
            IOUtils.closeStream(in)
        }
    }
    
    protected def parseRow(line: String, separator: String = ","): Unit
}

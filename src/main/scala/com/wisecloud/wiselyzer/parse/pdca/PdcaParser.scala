package com.wisecloud.wiselyzer.parse.pdca

import java.util.{Date, HashMap => JavaHashMap, Map => JavaMap}

import com.datastax.driver.core.{ConsistencyLevel, Session}
import org.apache.hadoop.fs.Path
import org.joda.time._
import org.joda.time.format.DateTimeFormat

import scala.collection.immutable.{HashMap => FixedHashMap, Map => FixedMap}
import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by hanlinux on 29/06/2017.
  */
class PdcaParser extends BaseParser with Serializable {

//    DP2,Version: BZ-02.00-25.41-71.84-01.04_1.2.1; BZ-02.01-25.41-71.84-01.04_1.2.1
//    Product,SerialNumber,Special Build Name,Special Build Description,Unit Number,Station ID,Test Pass/Fail Status,StartTime,EndTime,List Of Failing Tests,Version,DFP,DFVDown,DFVUp,DBHP,DBHVDown,DBHVUp,PBSCC,PBS_X,PBS_Z,VBSDownCC,VBSDown_X,VBSDown_Z,VBSUpCC,VBSUp_X,VBSUp_Z,PBSTC,PBSTB,Preload_P,VBSDownTC,VBSDownTB,Preload_Vdown,VBSUpTC,VBSUpTB,Preload_Vup,Pallet Barcode,Cycle Time,Start_OP,DP1_CT,DP2_CT,Pallet Pickup,NG_DHP,NG_DHVDown,NG_DHVUp,NG_DBP,NG_DBVDown,NG_DBVUp,NG_DFP,NG_DFVDown,NG_DFVUp,NG_PBSCC,NG_VBSDownCC,NG_VBSUpCC,BBS2_NZ_1_FAIL,BBS2_NZ_2_FAIL,BBS2_NZ_3_FAIL,Shim_toss,PB Shim Press Force,VUP Shim Press Force,VD Shim Press Force,Dwell,Operator_ID,Mode,TestSeriesID
//    Display Name ----->
//    PDCA Priority ----->,,,,,,,,,,,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
//    Upper Limit ----->,,,,,,,,,,,0.23,0.87,0.87,NA,NA,NA,0.09,NA,NA,0.09,NA,NA,0.09,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA
//    Lower Limit ----->,,,,,,,,,,,0.01,0.65,0.65,NA,NA,NA,0,NA,NA,0,NA,NA,0.00,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA
//    Measurement Unit ----->,,,,,,,,,,,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA
    
    private val station_version_row = 1
    private val header_row = 2
    private val displayName_row = 3
    private val pdcaPriority_row = 4
    private val upperLimit_row = 5
    private val LowerLimit_row = 6
    private val Unit_row = 7
    private val dataStart_row = 8
    
    //Raw Data Field Name
    //Product,SerialNumber,Special Build Name,Special Build Description,Unit Number,Station ID,
    //Test Pass/Fail Status,StartTime,EndTime,List Of Failing Tests,Version
    private val product_field = "Product".trim.toLowerCase
    private val sn_field = "SerialNumber".trim.toLowerCase
    private val specialBuildName_field = "Special Build Name".trim.toLowerCase
    private val specialBuildDesc_field = "Special Build Description".trim.toLowerCase
    private val unitNumber_field = "Unit Number".trim.toLowerCase
    private val stationId_field = "Station ID".trim.toLowerCase
    private val testStatus_field = "Test Pass/Fail Status".trim.toLowerCase
    private val startTime_field = "StartTime".trim.toLowerCase
    private val endTime_field = "EndTime".trim.toLowerCase
    private val failingList_field = "List Of Failing Tests".trim.toLowerCase
    private val version_field = "Version".trim.toLowerCase
    
    //Cassandra Table Field Name
    private val product_c_field = "Product".trim.toLowerCase
    private val sn_c_field = "SerialNumber".trim.toLowerCase
    private val specialBuildName_c_field = "Special_Build_Name".trim.toLowerCase
    private val specialBuildDesc_c_field = "Special_Build_Description".trim.toLowerCase
    private val unitNumber_c_field = "Unit_Number".trim.toLowerCase
    private val stationId_c_field = "Station_ID".trim.toLowerCase
    private val testStatus_c_field = "Test_Status".trim.toLowerCase
    private val startTime_c_field = "StartTime".trim.toLowerCase
    private val endTime_c_field = "EndTime".trim.toLowerCase
    private val failingList_c_field = "List_Of_Failing_Tests".trim.toLowerCase
    private val version_c_field = "Version".trim.toLowerCase
    
    private val phase_c_field = "phase".trim.toLowerCase
    private val npiConfig_c_field = "npiConfig".trim.toLowerCase
    private val segment_c_field = "segment".trim.toLowerCase
    private val factoryCode_c_field = "factoryCode".trim.toLowerCase
    private val floor_c_field = "floor".trim.toLowerCase
    private val lineId_c_field = "line_id".trim.toLowerCase
    private val machineId_c_field = "machine_id".trim.toLowerCase
    private val stationName_c_field = "station_name".trim.toLowerCase
    private val item_c_field = "item".trim.toLowerCase
    private val value_c_field = "value".trim.toLowerCase
    private val upper_c_field = "upper".trim.toLowerCase
    private val lower_c_field = "lower".trim.toLowerCase
    private val unit_c_field = "unit".trim.toLowerCase
    private val upper_c_field_map = "upper_map".trim.toLowerCase
    private val lower_c_field_map = "lower_map".trim.toLowerCase
    private val unit_c_field_map = "unit_map".trim.toLowerCase
    private val source_c_field = "csv_file".trim.toLowerCase
    
    private var hasVersionField = false
    private var headerIndexNameMap = mutable.HashMap[Int, String]()
    //private var headerNameIndexMap = mutable.HashMap[String, Int]()
    
    private var stationName = ""
    private var version = ""
    private var header: List[String] = Nil;
    private var displyName: List[String] = Nil;
    private var pdcaPriority: List[String] = Nil;
    private var upperLimit: List[String] = Nil;
    private var lowerLimit: List[String] = Nil;
    private var unit: List[String] = Nil;
    
    //SerialNumber,Special Build Name,Special Build Description,Unit Number,Station ID,Test Pass/Fail Status,StartTime,EndTime,List Of Failing Tests,Version
    private val commonFieldMap = FixedHashMap[String, String](
        product_field -> product_c_field,
        sn_field -> sn_c_field,
        specialBuildName_field -> specialBuildName_c_field,
        specialBuildDesc_field -> specialBuildDesc_c_field,
        unitNumber_field -> unitNumber_c_field,
        stationId_field -> stationId_c_field,
        testStatus_field -> testStatus_c_field,
        startTime_field -> startTime_c_field,
        endTime_field -> endTime_c_field,
        failingList_field -> failingList_c_field,
        version_field -> version_c_field
    )
    
    private val tableFieldMap = HashMap[String, String](
        product_c_field -> "",
        sn_c_field -> "",
        specialBuildName_c_field -> "",
        specialBuildDesc_c_field -> "",
        phase_c_field -> "",        //not ready
        npiConfig_c_field -> "",    //not ready
        segment_c_field -> "",      //not ready
        unitNumber_c_field -> "",
        stationId_c_field -> "", //FXGL_C03-5FT-02B_1_AE-22 => factoryCode, floor, line_id, machine_id
        factoryCode_c_field -> "",
        floor_c_field -> "",
        lineId_c_field -> "",
        machineId_c_field -> "",
        stationName_c_field -> "",   //DP2 from text file (R1:C1)
        testStatus_c_field -> "",
        startTime_c_field -> "",
        endTime_c_field -> "",
        failingList_c_field -> "",
        version_c_field -> "",
        item_c_field -> "",
        value_c_field -> "",
        upper_c_field -> "",
        lower_c_field -> "",
        unit_c_field -> "",
        source_c_field -> ""
    )
    
    private val metaTableFieldMap = HashMap[String, String](
        product_c_field -> "",
        sn_c_field -> "",
        specialBuildName_c_field -> "",
        specialBuildDesc_c_field -> "",
        phase_c_field -> "",        //not ready
        npiConfig_c_field -> "",    //not ready
        segment_c_field -> "",      //not ready
        unitNumber_c_field -> "",
        stationId_c_field -> "", //FXGL_C03-5FT-02B_1_AE-22 => factoryCode, floor, line_id, machine_id
        factoryCode_c_field -> "",
        floor_c_field -> "",
        lineId_c_field -> "",
        machineId_c_field -> "",
        stationName_c_field -> "",   //DP2 from text file (R1:C1)
        testStatus_c_field -> "",
        startTime_c_field -> "",
        endTime_c_field -> "",
        failingList_c_field -> "",
        version_c_field -> "",
        source_c_field -> ""
    )
    
    private val specTableFieldMap = HashMap[String, String](
        product_c_field -> "",
        stationId_c_field -> "", //FXGL_C03-5FT-02B_1_AE-22 => factoryCode, floor, line_id, machine_id
        factoryCode_c_field -> "",
        floor_c_field -> "",
        lineId_c_field -> "",
        machineId_c_field -> "",
        stationName_c_field -> "",   //DP2 from text file (R1:C1)
        version_c_field -> "",
        item_c_field -> ""
    )
    
    private val specTableUpdateFieldMap = HashMap[String, String](
        product_c_field -> "",
        stationId_c_field -> "",
        stationName_c_field -> "",
        startTime_c_field -> "",
        version_c_field -> "",
        item_c_field -> "",
        upper_c_field -> "",
        lower_c_field -> "",
        unit_c_field -> ""
    )
    
    lazy private val insertTableStr =
        "INSERT INTO " + this.keyspace + "." + this.tableName + " (" + tableFieldMap.keys.mkString(",") + ") " +
            "VALUES (" + List.fill(tableFieldMap.keys.size)("?").mkString(",") + ") " +
            ";"//"USING TIMESTAMP ? ;"
    
    lazy private val insertMetaTableStr =
        "INSERT INTO " + this.keyspace + ".meta_" + this.tableName + " (" + metaTableFieldMap.keys.mkString(",") + ") " +
            "VALUES (" + List.fill(metaTableFieldMap.keys.size)("?").mkString(",") + ") " +
            ";"//"USING TIMESTAMP ? ;"

    lazy private val insertSpecTableStr =
        "INSERT INTO " + this.keyspace + ".spec_" + this.tableName + " (" +
        specTableFieldMap.keys.filter(isMapField(_)).mkString(",") + ") " +
            "VALUES (" + List.fill(specTableFieldMap.keys.filter(isMapField(_)).size)("?").mkString(",") + ") " +
            " IF NOT EXISTS;"//"USING TIMESTAMP ? ;"
    
    private def isMapField(field: String): Boolean ={
        field match {
            case `upper_c_field` | `lower_c_field` | `unit_c_field` => false
            case _ => true
        }
    }
    
    private val updateSpecTableBaseStr =
        "UPDATE %s.spec_%s" +
        " SET %s = %s + :%s, %s = %s + :%s, %s = %s + :%s " +
        " WHERE %s = :%s and %s = :%s and %s = :%s and %s = :%s and %s = :%s "
        
    lazy private val updateSpecTableStr = updateSpecTableBaseStr.format(
        this.keyspace, this.tableName,
        upper_c_field, upper_c_field, upper_c_field_map,
        lower_c_field, lower_c_field, lower_c_field_map,
        unit_c_field, unit_c_field, unit_c_field_map,
        product_c_field, product_c_field,
        stationId_c_field, stationId_c_field,
        stationName_c_field, stationName_c_field,
        version_c_field, version_c_field,
        item_c_field, item_c_field)
    
    //lazy val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    
    override protected def parseRow(line: String, separator: String): Unit = {
        lineCount += 1
        val lineFieldList : List[String] = line.split(separator).toList.map(item => item.trim)
    
        lineCount match {
            case c if (c >= dataStart_row) => {
                parseData(lineFieldList)
            }
            case `station_version_row` => {
                stationName = lineFieldList(0)
    
                if (lineFieldList.size > 1) {
                    //Version: BZ-02.00-25.41-71.84-01.04_1.2.1; BZ-02.01-25.41-71.84-01.04_1.2.1
                    //  => BZ-02.00-25.41-71.84-01.04_1.2.1
                    version = lineFieldList(1).split(":")(1).split(";")(0).trim
                }
            }
            case `header_row` => {
                header = lineFieldList.map(fieldName => fieldName.trim.toLowerCase)
                
                //Mapping header index by header name
                setMapping(header.indexOf(product_field), product_field)
                setMapping(header.indexOf(sn_field), sn_field)
                setMapping(header.indexOf(specialBuildName_field), specialBuildName_field)
                setMapping(header.indexOf(specialBuildDesc_field), specialBuildDesc_field)
                setMapping(header.indexOf(unitNumber_field), unitNumber_field)
                setMapping(header.indexOf(stationId_field), stationId_field)
                setMapping(header.indexOf(testStatus_field), testStatus_field)
                setMapping(header.indexOf(startTime_field), startTime_field)
                setMapping(header.indexOf(endTime_field), endTime_field)
                setMapping(header.indexOf(failingList_field), failingList_field)
                
                if(header.indexOf(version_field) != -1) {
                    hasVersionField = true
                    setMapping(header.indexOf(version_field), version_field)
                }
    
                def setMapping(index: Int, name: String): Unit ={
                    headerIndexNameMap.put(index, name)
                   // headerNameIndexMap.put(name, index)
                }
            }
            case `displayName_row` => {
                displyName = lineFieldList
            }
            case `pdcaPriority_row` => {
                pdcaPriority = lineFieldList
            }
            case `upperLimit_row` => {
                upperLimit = lineFieldList
            }
            case `LowerLimit_row` => {
                lowerLimit = lineFieldList
            }
            case `Unit_row` => {
                unit = lineFieldList
            }
            case _ =>
        }
    }
    
    private def parseData(fieldList: List[String]): Unit ={
        //println(insertTableStr)
        //clear Map values
        List(tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap).foreach(fieldMap => {
            fieldMap.keys.foreach(field => {
                fieldMap.put(field, null)
            })
        })
    
        var fieldCount = -1;
        
        var commonFieldCount = 0;
        val commonMaxField = {
            if(this.hasVersionField)
                commonFieldMap.size
            else
                commonFieldMap.size - 1
        }
    
        if(!hasVersionField){
            setCassandratableFieldMap(version_c_field, this.version, tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
        }
    
        setCassandratableFieldMap(stationName_c_field, this.stationName, tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap);
        setCassandratableFieldMap(source_c_field, new Path(this.filePathString).getName, tableFieldMap, metaTableFieldMap)
    
        fieldList.foreach(colValue => {
            fieldCount += 1
    
            val rawCommonField = headerIndexNameMap.get(fieldCount)
    
            if (rawCommonField != None) {
                //SerialNumber,Special Build Name,Special Build Description,Unit Number,
                //Station ID,Test Pass/Fail Status,StartTime,EndTime,List Of Failing Tests,Version
                
                val rawCommonFieldStringName: String = rawCommonField.mkString
                
                if(commonFieldCount < commonMaxField && commonFieldMap.contains(rawCommonFieldStringName)) {
                    commonFieldCount += 1
                    
                    val cassandraTableFieldStringName: String = commonFieldMap.get(rawCommonFieldStringName).mkString
                    
                    setCassandratableFieldMap(cassandraTableFieldStringName, colValue, tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
                    
                    if (cassandraTableFieldStringName.equals(this.stationId_c_field)) {
                        //FXGL_C03-5FT-02B_1_AE-22 => factoryCode, floor, line_id, machine_id
                        //println(cassandraTableFieldStringName + ", " + this.stationId_c_field)
                        val it = colValue.split("_").iterator
    
                        setCassandratableFieldMap(factoryCode_c_field, getItValue(it), tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap);
                        setCassandratableFieldMap(floor_c_field, getItValue(it), tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap);
                        setCassandratableFieldMap(lineId_c_field, getItValue(it), tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap);
                        setCassandratableFieldMap(machineId_c_field, getItValue(it), tableFieldMap, metaTableFieldMap, specTableFieldMap, specTableUpdateFieldMap);
                        
                        def getItValue(it: Iterator[String]): String = {
                            if (it.hasNext)
                                it.next()
                            else
                                ""
                        }
                    }
                }
                
            } else {
                setCassandratableFieldMap(item_c_field, this.header(fieldCount), tableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
                setCassandratableFieldMap(value_c_field, colValue, tableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
                setCassandratableFieldMap(upper_c_field, this.upperLimit(fieldCount), tableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
                setCassandratableFieldMap(lower_c_field, this.lowerLimit(fieldCount), tableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
                setCassandratableFieldMap(unit_c_field, this.unit(fieldCount), tableFieldMap, specTableFieldMap, specTableUpdateFieldMap)
    
                //Insert Data
                conn.withSessionDo({ session =>
                    insrtDataToCassandra(session, insertTableStr, tableFieldMap)
                    insrtDataToCassandra(session, insertSpecTableStr, specTableFieldMap)
                    updateSpecDataToCassandra(session, updateSpecTableStr, specTableUpdateFieldMap)
                })
    
            }
        })
    
        conn.withSessionDo({ session =>
            insrtDataToCassandra(session, insertMetaTableStr, metaTableFieldMap)
        })
    
    }
    
    private def setCassandratableFieldMap(fieldName: String, fieldValue: String, fieldMaps: HashMap[String, String]*): Unit ={
        fieldMaps.foreach(fieldMap => {
            if(fieldMap.contains(fieldName)) {
                fieldMap.put(fieldName, fieldValue)
            }
        })
    }

    private def insrtDataToCassandra(session: Session ,sqlString: String, fieldMap: HashMap[String, String]): Unit ={
        val sessionPrepare = session.prepare(sqlString).setConsistencyLevel(ConsistencyLevel.ALL)
        val buond = sessionPrepare.bind()
        
        fieldMap.keys.foreach(
            fieldNameString =>
                fieldNameString match {
                    case this.startTime_c_field => {
                        val startTime = DateTime.parse(fieldMap.get(fieldNameString).mkString, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDate
                        //val startTime = DateTime.parse(fieldMap.get(colValue).mkString, dtf).toDate
                    
                        buond.setTimestamp(fieldNameString, startTime)
                        //buond.setLong("[timestamp]", startTime.getTime)
                    }
                    case this.endTime_c_field => {
                        buond.setTimestamp(fieldNameString, DateTime.parse(fieldMap.get(fieldNameString).mkString, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDate)
                        //buond.setTimestamp(fieldNameString, DateTime.parse(fieldMap.get(fieldNameString).mkString, dtf).toDate)
                    }
                    case _ => {
                        buond.setString(fieldNameString, fieldMap.get(fieldNameString).mkString)
                    }
                }
        )
    
        session.execute(buond)
    }
    
    private def updateSpecDataToCassandra(session: Session ,sqlString: String, fieldMap: HashMap[String, String]): Unit ={
        val sessionPrepare = session.prepare(sqlString).setConsistencyLevel(ConsistencyLevel.ALL)
        val buond = sessionPrepare.bind()
        
        fieldMap.keys.foreach(
            fieldNameString =>
                fieldNameString match {
                    case this.startTime_c_field => {
                        val startTime = DateTime.parse(fieldMap.get(fieldNameString).mkString, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDate
                        //val startTime = DateTime.parse(fieldMap.get(colValue).mkString, dtf).toDate
                        
                        val upperMap: JavaMap[String, Date]  = new JavaHashMap[String, Date]()
                        upperMap.put(fieldMap.get(upper_c_field).mkString, startTime);
                        buond.setMap[String, Date](upper_c_field_map, upperMap)
                        
                        val lowerMap: JavaMap[String, Date]  = new JavaHashMap[String, Date]()
                        lowerMap.put(fieldMap.get(lower_c_field).mkString, startTime);
                        buond.setMap[String, Date](lower_c_field_map, lowerMap)
                        
                        val unitMap: JavaMap[String, Date]  = new JavaHashMap[String, Date]()
                        unitMap.put(fieldMap.get(unit_c_field).mkString, startTime);
                        buond.setMap[String, Date](unit_c_field_map, unitMap)
                    }
                    case `product_c_field` | `stationId_c_field` | `stationName_c_field` | `version_c_field` | `item_c_field` => {
                        buond.setString(fieldNameString, fieldMap.get(fieldNameString).mkString)
                    }
                    case _ =>
                }
        )
        
        session.execute(buond)
    }
    
    private def showLog(data: List[String]): Unit ={
        println(stationName)
        println(version)
        println(header)
        println(headerIndexNameMap)
       // println(headerNameIndexMap)
        println(displyName)
        println(pdcaPriority)
        println(upperLimit)
        println(lowerLimit)
        println(data)
    
//        DP2
//        BZ-02.00-25.41-71.84-01.04_1.2.1
//        List(Product, SerialNumber, Special Build Name, Special Build Description, Unit Number, Station ID, Test Pass/Fail Status, StartTime, EndTime, List Of Failing Tests, Version, DFP, DFVDown, DFVUp, DBHP, DBHVDown, DBHVUp, PBSCC, PBS_X, PBS_Z, VBSDownCC, VBSDown_X, VBSDown_Z, VBSUpCC, VBSUp_X, VBSUp_Z, PBSTC, PBSTB, Preload_P, VBSDownTC, VBSDownTB, Preload_Vdown, VBSUpTC, VBSUpTB, Preload_Vup, Pallet Barcode, Cycle Time, Start_OP, DP1_CT, DP2_CT, Pallet Pickup, NG_DHP, NG_DHVDown, NG_DHVUp, NG_DBP, NG_DBVDown, NG_DBVUp, NG_DFP, NG_DFVDown, NG_DFVUp, NG_PBSCC, NG_VBSDownCC, NG_VBSUpCC, BBS2_NZ_1_FAIL, BBS2_NZ_2_FAIL, BBS2_NZ_3_FAIL, Shim_toss, PB Shim Press Force, VUP Shim Press Force, VD Shim Press Force, Dwell, Operator_ID, Mode, TestSeriesID)
//        Map(8 -> endtime, 2 -> special build name, 5 -> station id, 4 -> unit number, 7 -> starttime, 10 -> version, 1 -> serialnumber, 9 -> list of failing tests, 3 -> special build description, 6 -> test pass/fail status, 0 -> product)
//        Map(endtime -> 8, test pass/fail status -> 6, special build name -> 2, product -> 0, starttime -> 7, unit number -> 4, list of failing tests -> 9, station id -> 5, serialnumber -> 1, version -> 10, special build description -> 3)
//        List(Display Name ----->)
//        List(PDCA Priority ----->, , , , , , , , , , , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
//        List(Upper Limit ----->, , , , , , , , , , , 0.23, 0.87, 0.87, NA, NA, NA, 0.09, NA, NA, 0.09, NA, NA, 0.09, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA)
//        List(Lower Limit ----->, , , , , , , , , , , 0.01, 0.65, 0.65, NA, NA, NA, 0, NA, NA, 0, NA, NA, 0.00, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA)
//        List(D22, C3970130009HP2NA6, , , , FXGL_C03-5FT-02B_1_AE-22, PASS, 2017-01-14 15:31:30, 2017-01-14 15:31:30, , BZ-02.01-25.41-71.84-01.04_1.2.1, .121, .769, .754, .568, 1.293, 1.299, .058, .058, .004, .046, .019, -.042, .068, -.047, -.049, .447, .47, .023, .524, .53, .00600000000000001, .545, .53, .015, 111228320010, 59.84, 152932, 27.04, 32.8, 20170114153131, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .82, .77, .78, 2, 1, 0, 0)
    }
}

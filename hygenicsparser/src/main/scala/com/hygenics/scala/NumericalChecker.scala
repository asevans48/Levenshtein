package com.hygenics.scala

import scala.collection.JavaConversions._
import scala.util.Try

import breeze.linalg._
import breeze.linalg.DenseMatrix._

import com.hygenics.parser.getDAOTemplate

import javax.annotation.Resource

import scala.beans._
import org.springframework.beans.factory.annotation.Required

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import org.slf4j.{Logger,LoggerFactory}
import java.lang.NullPointerException


import org.apache.commons.lang3.exception.ExceptionUtils

import scala.concurrent.{Await,Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable.HashSet

import sbt.IO

final class DuplicateError(obj:String) extends java.lang.Error(obj)
final class CountError(obj:String) extends java.lang.Error(obj)
final class JoinError(obj:String) extends java.lang.Error(obj)

/**
 * This class performs checking of counts, looks for full drops, and performs any other required numerical task.
 * 
 * It is intended to add analysis to the crawl/parse system so that other teams are less burdened in QA. 
 * 
 * The class features:
 * 
 * A. full drop and add checking
 * B. OLS (linear regerssion) checking --> Matrix form is used since this stuff is small. If it gets too large, use gradient descent.
 * C. The ability to halt a parse
 * 
 * The default halt on issue status can be overridden via the haltParse parameter
 * 
 * Counts come for a specified table, automatically specified as countTable and a default columnTable 
 * 
 * @author aevans
 */
class NumericalChecker {
  
  private var log:Logger = LoggerFactory.getLogger(this.getClass.getName)
  
  @BeanProperty
  var ignoreAllTables: Boolean  = false;
	
  @BeanProperty
  var ignoreAllColumns: Boolean  = false;
  
  @BeanProperty
  var haltParse:Boolean = true
  
  @BeanProperty
  var countTable:String = "data.counts"
  
  @BeanProperty
  var columnTable:String ="data.columns"
  
  @Required
  @Resource(name="getDAOTemplate")
  @BeanProperty
  var jdbcTemplate:getDAOTemplate = _
  
  @BeanProperty
  var schema:String = _
  
  @BeanProperty
  var countDif:Double = 100.0
  
  @BeanProperty
  var colCountDif:Double = 100.0
  
  @BeanProperty
  var killOnDuplicates:Boolean = true
  
  @BeanProperty
  var distinctColumns: java.util.Map[String,java.util.List[String]] = null
  
  @BeanProperty
  var ignoreTableContains:String = ".*?dirty.*"
  
  @BeanProperty
  var joinTables:java.util.List[String] = _ //Perform requested joins and ensure that more than one record actually joins
  
  
  @BeanProperty
  var checkFiles:java.util.Map[String,java.util.Map[String,String]] = _ //Check the directory to ensure that file names match a give field
  
  @BeanProperty
  var dirDiff: Int = 100
  
  @BeanProperty
  var avoidanceString:String = null
  
  @BeanProperty
  var termtime: Int = 30000
  
  @BeanProperty
  var procMul: Int = 1000
  
  @BeanProperty
  var ignoreColumns: String = null
  
  /**
   * Performs Ordinary least squares on each array in the map. Returns a new Mapping representing equations for expected values.
   */
  def OLS(data:Map[String,Array[Double]]):Map[String,List[Double]]={
    var eqVars:Map[String,List[Double]] = Map[String,List[Double]]()
    //use the matrix formulation for
    
    data.foreach{
      dataKV => 
        log.info("Checking Trends for "+dataKV._1)
        //get data
         var table:String = dataKV._1
         var y = new DenseVector(dataKV._2)
         
         var xrange = 0 to dataKV._2.length-1
         var xmat =new DenseVector(xrange.map { x => x * 1.0 }.toArray)
         var b1 = xmat * xmat.t
         b1.map { x => math.pow(x, -1) }
         var b2 = xmat.t * y
         var vars = b1 * b2
         eqVars = eqVars + (table -> List(vars.data.apply(0),vars.data.apply(1)))
    }
    eqVars
  }
  
  
  /**
   * Checks trends using OLS (equations work after testing, will implement when a few more come in
   * Deleted from REPO. NEED TO REWRITE 
   * 
   */
  def checkTrends(data:Map[String,List[Double]]):Boolean={
    //TODO Needs a gradient decent. Maybe somewhere but seems to have been deleted. Backup option is matrix calc.
    
    //get trendline equations
    
    //val trends = OLS(convertToArray(jdbcTemplate.getJsonData("SELECT tablename,date,count FROM data.counts WHERE schemaname ILIKE '%"+schema+"%' ORDER BY tablename,date DESC")))
    
    //check current counts against the trend line and error when appropriate
    false
  }

  
  /**
   * Checks the directly previous run for additions or complete drops. 
   * These are table counts
   */
  def getCounts():java.util.ArrayList[String]={
      //get the last counts
      if(ignoreTableContains == null){
        jdbcTemplate.getJsonData(s"SELECT tablename,date,count FROM $countTable WHERE schemaname ILIKE '$schema'")
      }else{
        jdbcTemplate.getJsonData(s"SELECT tablename,date,count FROM $countTable WHERE schemaname ILIKE '$schema' and tablename NOT ILIKE '%$ignoreTableContains%'")
      }
  }
 
  
  /**
   * Gets the Last counts from the most recent runs
    */ 
  def getPreviousCounts():java.util.ArrayList[String]={
    if(ignoreTableContains == null){
      jdbcTemplate.getJsonData("SELECT tablename,date,count FROM (SELECT tablename,date,count, rank() OVER (partition by tablename ORDER BY date DESC)  FROM (SELECT tablename,date,count  FROM "+this.countTable+" WHERE schemaname ILIKE '"+schema+"' ORDER BY tablename,date DESC) as q1) as q2 WHERE rank = 1 AND date_trunc('day',date) IN (SELECT max(date_trunc('day',date)) FROM data.counts WHERE schemaname ILIKE '"+schema+"')")
    }else{
      jdbcTemplate.getJsonData(s"SELECT tablename,date,count FROM (SELECT tablename,date,count, rank() OVER (partition by tablename ORDER BY date DESC)  FROM (SELECT tablename,date,count  FROM $countTable WHERE schemaname ILIKE '$schema' ORDER BY tablename,date DESC) as q1) as q2 WHERE rank = 1 AND date_trunc('day',date) IN (SELECT max(date_trunc('day',date)) FROM data.counts WHERE schemaname ILIKE '$schema' AND tablename NOT ILIKE '%$ignoreTableContains%')")
    }
  }
  
  /**
   * Get the last runs column counts
   */
  def getPreviousColumnCounts():java.util.ArrayList[String]={
     if(this.ignoreTableContains == null){
       jdbcTemplate.getJsonData("SELECT tablename,columnname,date,count FROM (SELECT tablename,date,count,columnname, rank() OVER (partition by tablename,columnname ORDER BY date DESC)  FROM (SELECT tablename,date,count,columnname  FROM "+this.columnTable+" WHERE schemaname ILIKE '"+schema+"' ORDER BY tablename,date DESC) as q1) as q2 WHERE rank = 1 AND date_trunc('day',date) IN (SELECT max(date_trunc('day',date)) FROM data.counts WHERE schemaname ILIKE '"+schema+"')")
     }else{
       jdbcTemplate.getJsonData(s"SELECT tablename,columnname,date,count FROM (SELECT tablename,date,count,columnname, rank() OVER (partition by tablename,columnname ORDER BY date DESC)  FROM (SELECT tablename,date,count,columnname  FROM $columnTable WHERE schemaname ILIKE '$schema' ORDER BY tablename,date DESC) as q1) as q2 WHERE rank = 1 AND date_trunc('day',date) IN (SELECT max(date_trunc('day',date)) FROM data.counts WHERE schemaname ILIKE '$schema' AND tablename NOT ILIKE '%$ignoreTableContains%')")
     }
  }
  
  /**
   * Get the column Counts for all runs
   */
  def getColumnCounts():java.util.ArrayList[String]={
    if(this.ignoreTableContains == null){
      jdbcTemplate.getJsonData("SELECT tablename,date,count FROM "+this.columnTable+" WHERE schemaname ILIKE '"+schema+"'")
    }else{
      jdbcTemplate.getJsonData(s"SELECT tablename,date,count FROM $columnTable WHERE schemaname ILIKE '$schema' AND tablename NOT ILIKE '%$ignoreTableContains%'")
    }
  }
  

  def getJsonsAsMapWithList(data:java.util.ArrayList[String]):Map[String,List[Double]]={
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    var mp:Map[String,List[Double]] = Map[String,List[Double]]()
    data.foreach { x =>
      var jmap:Map[String,String]=mapper.readValue[Map[String,String]](x)
      if(mp.containsKey(jmap.get("tablename").get)){
         mp = mp + (jmap.get("tablename").get-> mp.get("tablename").get.:+(jmap.get("count").get.toInt*1.0))
      }else{
        mp = mp + (jmap.get("tablename").get -> List(jmap.get("count").get.toInt*1.0))
      }
      
    }
    mp
  }
  
  def getJsonsAsMap(data:java.util.ArrayList[String]):Map[String,Map[String,List[Double]]]={
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    var mp:Map[String,Map[String,List[Double]]] = Map[String,Map[String,List[Double]]]()
    data.foreach { x =>
      var jmap:Map[String,String]=mapper.readValue[Map[String,String]](x)
      if(mp.containsKey(jmap.get("tablename").get)){
         var tmap = mp.get(jmap.get("tablename").get).get
         tmap.keySet.foreach{
           k => 
             tmap = tmap + (k -> tmap.get(k).get.:+(jmap.get(k).get.toInt*1.0))
         }
         mp = mp.updated(jmap.get("tablename").get, tmap)
      }else{
        var tmap:Map[String,List[Double]] = Map[String,List[Double]]()
        jmap.keySet.foreach { k => 
           tmap = tmap + (k -> List(jmap.get(k).get.toInt*1.0))
        }
        mp = mp + (jmap.get("tablename").get -> tmap)
      }
      
    }
    mp
  }
  
  
  def getColumnJsonsAsMap(data:java.util.ArrayList[String]):Map[String,Map[String,List[Double]]]={
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    var mp:Map[String,Map[String,List[Double]]] = Map[String,Map[String,List[Double]]]()
    data.foreach { x =>
      var jmap:Map[String,String]=mapper.readValue[Map[String,String]](x)
      if(mp.containsKey(jmap.get("tablename").get)){
         var tmap = mp.get(jmap.get("tablename").get).get
         
         if(tmap.containsKey(jmap.get("columnname").get)){
             tmap = tmap.updated(jmap.get("columnname").get, tmap.get("columnname").get :+ (jmap.get("count").get.toInt * 1.0)) 
         }else{
             tmap = tmap + (jmap.get("columnname").get -> List(jmap.get("count").get.toInt * 1.0)) 
         }
         
         
         mp = mp.updated(jmap.get("tablename").get, tmap)
      }else{
        var tmap:Map[String,List[Double]] = Map[String,List[Double]]()
        tmap = tmap + (jmap.get("columnname").get -> List(jmap.get("count").get.toInt))
        mp = mp + (jmap.get("tablename").get -> tmap)
      }
      
    }
    mp
  }
  
  
  
  def getTableCount(table:String):Int={
    jdbcTemplate.getCount(table)
  }

  /**
   * Check if column counts are appropriate
   */
  def checkColumns(table: String,columns:Map[String,List[Double]]):Boolean={
    if(this.ignoreTableContains == false || !table.matches(this.ignoreTableContains)){
      val mapper=new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      var bool:Boolean = false
      columns.keySet.foreach{
        col =>
           if(mapper.readValue[Map[String,Int]](jdbcTemplate.getJsonData(s"SELECT count(*) FROM information_schema.tables WHERE table_schema ILIKE '$schema' AND table_name ILIKE '$table'").get(0)).get("count").get > 0){

              val mapper=new ObjectMapper() with ScalaObjectMapper
              mapper.registerModule(DefaultScalaModule)
              //check counts if table exists
              var jmap:Map[String,Int] = mapper.readValue[Map[String,Int]](jdbcTemplate.getJsonData("SELECT count(*) FROM "+schema+"."+table+" WHERE "+col+" IS NOT NULL AND length(trim(cast("+col+" as text))) > 0").get(0))
              val ct = jmap.values.toList.get(0) 
              var cck = ct / columns.get(col).get(0)
              
              if(cck > 1.0){
                cck = 2.0 - cck
              }
              
              if(!(ignoreColumns != null && col.matches(ignoreColumns)) && ((colCountDif > 1.0 && math.abs(ct - columns.get(col).get(0)) >= colCountDif) || (colCountDif <= 1.0 && (cck < colCountDif)))){
                bool=true
                log.error("COLUMN COUNT ERROR: "+col+"\nTABLE: "+table+"\nExpected: "+columns.get(col).get(0)+"\nACTUAL: "+ct)
                log.error("ERROR COUNT PERCENTAGE: %.02f".format(cck))
              }else if(ignoreColumns != null && col.matches(ignoreColumns)){
                log.info("Ignoring Column: "+col+" on table "+table)
              }
            }else{
              bool=true
              log.error(s"COLUMN EXISTANCE ERROR: Column Does not Exist \nColumn: $col\nTable: $table")
            }
          
        }
      bool
    }else{
      false
    }
  }
  
  /**
   * Check that the joins actually produce records
   */
  def checkJoin(sql:String):Future[Boolean]=Future{
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    try{
     mapper.readValue[Map[String,Int]](this.jdbcTemplate.getJsonData(sql).get(0)).get("count").get > 0
    }catch{
      case e:Throwable => {
        log.error("JOIN Exception (Query Expects a Count):\nSQL"+sql+"\n"+e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
        false
      }
    }
  }
  
  

  def checkDirectory(directory:String,keys:java.util.Map[String,String]):Future[Boolean]=Future{
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    
    def tableContains(sql:String):Future[Int]=Future{
      mapper.readValue[Map[String,Int]](this.jdbcTemplate.getJsonData(sql).get(0)).get("count").get
    }
    
    var bool: Boolean = true
    //get original count
    val field:String = keys.get("field")
    val table:String = keys.get("table")
    val ct = mapper.readValue[Map[String,Int]](this.jdbcTemplate.getJsonData(s"SELECT count(*) FROM $table").get(0)).get("count").get

    var fct=0
    var noMatch=0
    var futs:List[Future[Int]] = List[Future[Int]]()
   
    val imgs =this.jdbcTemplate.getArrayList(s"SELECT $field FROM $table", field)
    
    //generate lookup table
    val set:Set[String] = Set.apply(imgs:_*)
    
    //get paths
    var ioSet:Set[String] = if(keys.containsKey("replace")) Set.apply(IO.listFiles(new java.io.File(directory)).map { x => x.getName.replaceAll("\\..*","") }:_*) else Set.apply(IO.listFiles(new java.io.File(directory)).map { x => x.getName }:_*)
    
    if(avoidanceString != null){
      ioSet = ioSet.filter({ str => !str.matches(avoidanceString) })
    }
    
    fct = (set & ioSet).size
    
    log.info(s"For Directory $directory\nTotal Files: $ct\nTotal Joining Count: $fct\nNon-Matching Files: "+(ct-fct)+"\nOutcome: "+(Math.abs(ct - fct) < dirDiff))
    Math.abs(ct - fct) < dirDiff
  }
  
  /**
   * Checks for Duplicate Records
   */
  def checkDuplicates():Boolean={
    var pass:Boolean = false
    distinctColumns.keySet.foreach{
      table =>
      var keys:String = ""
      distinctColumns.get(table).foreach{
        k => keys = if(keys.length == 0 ) k else keys+","+k
      }
      var schemas = jdbcTemplate.getArrayList(s"SELECT * FROM (SELECT (count(distinct($keys)) - count(*)) as ct FROM ${schema}.${table}) as q1","ct")
      if(schemas != null){
        var tpass = schemas.get(0).equals("0")
        if(!tpass){
            if(this.killOnDuplicates){
              pass=true
            }
            
           log.error(s"QA ISSUE: Duplicates Exist for a table!\nTable: ${table}\nWill terminate!")
           log.error("Number of Duplicates: "+schemas.get(0))
         }
       }else{
         log.error("Schema or Keys Not Found In Duplicate Check")
         try{
           throw new NullPointerException("Schema or Keys Not Found In Duplicate Check")
         }catch{
           case e:Throwable => log.error(e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
         }
       }
    }
    pass
  }
  
  /**
   * Gets Counts from the database.
   */
  def checkCounts()={
    var err:Boolean = false
    
    //Check for distincts
    if(this.distinctColumns != null){
       err=checkDuplicates
    }
    
    if(err){
      try{
        throw new DuplicateError("Duplicates Exist")
      }catch{
        case e:Throwable => log.error(e.getMessage +"\n"+ExceptionUtils.getStackTrace(e))
        sys.exit(1)
      }
    }
    
    //check table counts
    if(!this.ignoreAllTables){
      val cts = getJsonsAsMapWithList(getPreviousCounts())
      cts.keySet.foreach{
        x => 
        val table = if(x.contains(this.schema)) x else this.schema+"."+x
        if(this.ignoreTableContains == null || table.matches(this.ignoreTableContains) == false){
          val ctr=jdbcTemplate.getJsonData(s"SELECT count(*) as count FROM information_schema.tables WHERE table_schema||'.'||table_name ILIKE '$table'")
          if("""(?i)count[^\:=]+?[^0-9A-Za-z]+0[^0-9]+""".r.findFirstIn(ctr.get(0)).getOrElse(null) == null){
            var ct=getTableCount(table)
            var cck = (ct / cts.get(x).get(0))
            if(cck > 1.0){
               cck = 2.0 - cck
            }
            log.info("Count Percentage for %s Comparison:  %.02f".format(x,cck))
            
            if((countDif > 1.0 && (1.0 * math.abs(cts.get(x).get(0) - ct)) >= countDif) || (countDif <= 1.0 && (cck < countDif)) || (cts.get(x).get(0) == 0 && ct != 0)){
              err=true
              log.error("TABLE COUNT ERROR:\nTable: "+x+"\nExpected Within 100: "+cts.get(x).get(0)+"\nActual: "+ct)
            }
          }else{
            log.error(s"TABLE DOES NOT EXIST: $table")
            err= true
          }
        }else{
          log.info(s"Ignoring Table $table")
        }
      }
    }
    
    
    if(err){
      try{
        throw new CountError("TABLE COUNTS DO NOT MATCH")
      }catch{
        case e:Throwable => log.error(e.getMessage +"\n"+ExceptionUtils.getStackTrace(e))
        sys.exit(1)
      }
    }
    
    //check last column counts
    if(!this.ignoreAllColumns){
      val colCt = getColumnJsonsAsMap(this.getPreviousColumnCounts())
      colCt.keySet.foreach{x =>
          if(ignoreTableContains == null || !x.matches(ignoreTableContains)){
            val ck = checkColumns(x,colCt.get(x).get)
            if(ck){
              err = true
            }
          }else if(ignoreTableContains != null){
            log.info("Ignoring Columns for Table "+x)
          }
      }
    }
    
    if(err){
      try{
        throw new CountError("TABLE COLUMN COUNTS DO NOT MATCH")
      }catch{
        case e:Throwable => log.error(e.getMessage +"\n"+ExceptionUtils.getStackTrace(e))
        sys.exit(1)
        
      }
    }
    
    //check the joins
    if(this.joinTables != null){
      log.info("Checking Table Joins")
       var flist:List[Future[Boolean]] = List[Future[Boolean]]()
       this.joinTables.foreach { 
         sql =>{
           flist = flist :+ checkJoin(sql)
           
           if(flist.size > Runtime.getRuntime.availableProcessors()){
              val tbool = Await.result(Future.sequence(flist),Duration.Inf).contains(false)
              if(tbool){
                err=true
              }
              flist = List[Future[Boolean]]()
           }
         }
       }
       
       if(flist.size > 0){
         val tbool = Await.result(Future.sequence(flist),Duration.Inf).contains(false)
         if(tbool){
           err = true
         }
       }
       
       if(err){
        try{
          throw new JoinError("TABLES FAILED TO JOIN!")
        }catch{
          case e:Throwable => log.error(e.getMessage +"\n"+ExceptionUtils.getStackTrace(e))
          sys.exit(1)
        }
       }
       
    }
    
    //check for file matches
    if(this.checkFiles != null){
      log.info("Checking for File Match")
      var flist:List[Future[Boolean]] = List[Future[Boolean]]()
      checkFiles.keySet.foreach { 
        key =>{
          flist = flist :+ checkDirectory(key,this.checkFiles.get(key))
           
           if(flist.size > Runtime.getRuntime.availableProcessors()){
              val tbool = Await.result(Future.sequence(flist),Duration(this.termtime*Runtime.getRuntime.availableProcessors()*100000,"millis")).contains(false)
              if(tbool){
                err=true
              }
              flist = List[Future[Boolean]]()
           }
         }
       }
       
       if(flist.size > 0){
         val tbool = Await.result(Future.sequence(flist),Duration.Inf).contains(false)
         if(tbool){
           err = true
         }
       }
       
       if(err){
        try{
          throw new JoinError("Files Failed to Match!")
        }catch{
          case e:Throwable => log.error(e.getMessage +"\n"+ExceptionUtils.getStackTrace(e))
          sys.exit(1)
        }
       }
       
    }
  }
  
  def run()={
    checkCounts()
  }
  
}
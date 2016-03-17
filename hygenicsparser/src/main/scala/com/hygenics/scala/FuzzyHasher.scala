package com.hygenics.scala

import scala.concurrent.{Future,Await}
import scala.concurrent.duration.Duration

import com.hygenics.distance.Levenshtein;

import org.springframework.beans.factory.annotation.{Autowired,Required}
import scala.beans._

import com.hygenics.parser.getDAOTemplate
import scala.collection.JavaConversions._

import org.slf4j.{Logger,LoggerFactory}
import com.hygenics.parser.MainApp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.concurrent.{Future,Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure}

import java.util.Calendar

/**
 * A fuzzy hashing algorithm that can be used to eliminate duplicates faster and more efficiently than the 
 * Fuzzy match step in Pentaho. It currently supports Levenshtein only.  Batch Iteration is done with SQL.
 * 
 * Set an average allowed distance to not consider as hashed, set the columns
 */
class FuzzyHasher {
  
  var log:Logger = LoggerFactory.getLogger(classOf[MainApp])
  
  @BeanProperty
  @Autowired
  var template:getDAOTemplate = _
  
  @BeanProperty
  var distType : String = "levenshtein"
  
  /**
   * The sql to use to get data. This should be grouped by the column.
   */
  @Required
  @BeanProperty
  var sql:String = _
  
  
  /**
   * The column to group on.
   */
  @Required
  @BeanProperty
  var groupColumns: java.util.ArrayList[String] = _
  
  /**
   * The distance to allow.
   */
  @Required
  @BeanProperty
  var hashDist: Double = 0.0
  
  @BeanProperty
  var offset:Integer = 0
  
  @BeanProperty
  var batchSize:Integer = 100
  
  @Required
  @BeanProperty
  var hashColumn : String = _
  
  @Required
  @BeanProperty
  var idColumn : String = _
  
  @BeanProperty
  var extraCondition : String = _
  
  @BeanProperty
  var markOnly : Boolean = false
  
  @BeanProperty
  var outputTable : String = _
  
  @BeanProperty
  var duration = 120
  
  @BeanProperty
  var retries: Integer = 2
  
  /**
   * Parse the batch of data.
   * 
   * @param			mappings		The mappings to parse
   * @return 		A Future[List[String]] of jsons to be posted to the database
   */
  def getBatchHash( mappings : List[Map[String,String]]):Future[List[String]]=Future{
    
    var uniqueRecords:List[Map[String,String]] = List[Map[String,String]]()
    
    mappings.foreach{
      mapping => 
        
        var j:Integer = 0
        var run:Boolean = true
        var record: Map[String,String] = Map[String,String]()
        while(run && j < uniqueRecords.size){
          record  = uniqueRecords(j)
          var avgLev : Double = 0
          for(i <- 0 to groupColumns.size){
            val colName = groupColumns.get(i)
            avgLev += Levenshtein.getLevenshteinDistance(record.get(colName).get, mapping.get(colName).get)
          }
          
          if(avgLev / groupColumns.size() > this.hashDist){
             run = false
          }
        }
        
        if(run){
          uniqueRecords = uniqueRecords :+ record
        }
    }
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    uniqueRecords.map({r => mapper.writeValueAsString(r + ("table" -> this.outputTable))})
  }
  
  
  /**
   * Batch post (if > 1 record) a parsed batch of jsons
   * 
   * @param		json		The json string
   * @return	A Future[Any]
   */
  def postData(json: List[String])=Future{
    var arrList = new java.util.ArrayList[String]()
    json.foreach { x => arrList.add(x) }
    template.postJsonDatawithTable(arrList)
  }
  
  def run()={
    log.info("Starting Fuzzy Hasher @ "+Calendar.getInstance.getTime.toString)
    val dur= if(this.duration < 0) Duration.Inf else Duration(this.duration,"seconds")
    var data:java.util.ArrayList[String] = new java.util.ArrayList[String]()
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    
    do{
      //build query
      var query = sql
      var max :Integer = offset + batchSize
      sql += s"WHERE $idColumn > $offset AND $idColumn < $max" 
      
      if(extraCondition != null){
        sql += s" $extraCondition"
      }
      
      //get data
      data = template.getAll(query)
      var dataMap:Map[String,List[Map[String,String]]]= Map[String,List[Map[String,String]]]()
        
      //process
      data.foreach {  
        json => 
          val jmap:Map[String,String] = mapper.readValue[Map[String,String]](json)
          val offid = jmap.get(this.hashColumn).get
          if(dataMap.contains(offid)){
            dataMap = dataMap.updated(offid, dataMap.get(offid).get :+ jmap)
          }else{
            dataMap = dataMap + (offid -> List(jmap))
          }
      }
      
      //parse mappings, required key map makes .andThen untenable without mapping
      var procList:List[Future[Any]] = List[Future[Any]]()
      dataMap.keySet.foreach {  
        k =>
          procList = procList :+ getBatchHash(dataMap.get(k).get).map { x => postData(x)}
      }
      
      
      val r = Await.ready(Future.sequence(procList),dur)
      r onComplete{
        case Failure(t) => log.error(Calendar.getInstance.getTime.toString+" :: Case Completed With Failure will Retry After This Iteration $retries times.")
        case _ => log.info("Continuing") 
      }
      
      //increment batch
      offset += batchSize
    }while(data.size > 0)
    
      
      
    //look for data that may not have been inserted
    if(this.retries > 0){
      log.info(Calendar.getInstance.getTime.toString+"Looking for Missed Records.")
      var query = sql
      if(extraCondition != null){
        sql += s"WHERE $extraCondition"
      }
      
      var att:Integer = 0
      data = template.getAll(query)
      while(data.size() > 0 && att < this.retries){
        var dataMap:Map[String,List[Map[String,String]]]= Map[String,List[Map[String,String]]]()
        
        //process
        data.foreach {  
          json => 
            val jmap:Map[String,String] = mapper.readValue[Map[String,String]](json)
            val offid = jmap.get(this.hashColumn).get
            if(dataMap.contains(offid)){
              dataMap = dataMap.updated(offid, dataMap.get(offid).get :+ jmap)
            }else{
              dataMap = dataMap + (offid -> List(jmap))
            }
        }
        
        //create process list
        var procList:List[Future[Any]] = List[Future[Any]]()
        dataMap.keySet.foreach {  
          k =>
          procList = procList :+ getBatchHash(dataMap.get(k).get).map { x => postData(x)}
        }
      
        
        val r = Await.ready(Future.sequence(procList),dur)
        r onComplete{
          case Failure(t) => log.error(Calendar.getInstance.getTime.toString+"Case Completed With Failure will Retry After This Iteration $retries times.")
          case _ => log.info("Continuing") 
        }
        
        
        data = template.getAll(query)
        att += 1
      }
      
    }else{
      log.warn("Number of Retries set to 0.")
    }
    log.info("Completed Fuzzy Hasher @ "+Calendar.getInstance.getTime.toString)
  }
}
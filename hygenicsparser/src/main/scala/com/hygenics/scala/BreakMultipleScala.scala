package com.hygenics.scala

import java.util.{Calendar,Date,Set,Collection,HashSet,ArrayList}
import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import javax.annotation.{Resource}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


import com.hygenics.parser.getDAOTemplate
import org.springframework.context.annotation.{Bean}
import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.{Autowired}

import scala.beans._
import scala.collection.TraversableOnce
import scala.collection.JavaConversions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future,Await}
import scala.concurrent.duration._
import scala.util.{Success,Failure}
import org.slf4j.{LoggerFactory,Logger}

import org.springframework.beans.factory.annotation.Required

class BreakMultipleScala{ 
 
  @Required
  @BeanProperty 
  @Resource(name="getDAOTemplate") 
  var getDAOTemplate:getDAOTemplate = null
  
  @BeanProperty
  var hashName:String = "hash"
  
  @BeanProperty 
  var hashKey:String = "offenderhash"
  
  @BeanProperty 
  var unescape:Boolean = false
  
  @BeanProperty 
  var truncate:Boolean = true 
  
  @BeanProperty 
  var extracondition:String = null
  
  @BeanProperty 
  var fkeyref:String = null
  
  @BeanProperty 
  var key:String = null
  
  @BeanProperty 
  var foreignKey:String = null
  
  @BeanProperty 
  var repeatkeys:Boolean = false
  
  @BeanProperty 
  var loops:Int = 0
  
  @BeanProperty 
  var mustcontain:String = null 
  
  @BeanProperty 
  var cannotcontain:String = null
  
  @BeanProperty 
  var rowcolumn:String = null
  
  @BeanProperty 
  var qnum:Int = 5
  
  @BeanProperty 
  var maxpos:Int = Int.MaxValue
  
  @BeanProperty 
  var idcolumn:String = null
  
  @BeanProperty 
  var offset:Int = 0
  
  @BeanProperty
  var pullsize:Int = 100
  
  @BeanProperty 
  var commit_size:Int = 100
  
  @BeanProperty 
  var select:String = null
  
  @Required
  @BeanProperty 
  var targettable:String = _
  
  @BeanProperty 
  var replacementPattern:String = null
  
  @BeanProperty 
  var positions:java.util.Map[Int,String] = null
  
  @BeanProperty 
  var index:java.util.Map[String,List[String]] = null //only one key allowed in the map (key = name)
  
  @Required
  @BeanProperty 
  var token:String = "\\|"
  
  @BeanProperty
  var notnull:String = null
  
  @BeanProperty
  var checkstring:String = null
  
  @BeanProperty
  var numReattempts:Int = 5
  
  @BeanProperty
  var termtime:Long = 15000
  
  @BeanProperty
  var multiKeys:Map[String,List[String]] = null
  
  final var log:Logger = LoggerFactory.getLogger(this.getClass.getName)
  
  def BreakHandler(jsons:List[String]):Future[List[String]]=Future{
    var outlist:List[String] = List[String]()
    val mapper=new ObjectMapper() with ScalaObjectMapper
     mapper.registerModule(DefaultScalaModule)
     try{
        jsons.foreach { json => 
          var jmap:Map[String,String]=mapper.readValue[Map[String,String]](json)
          var output: Map[String,String]= Map[String,String]()
          
          val keys=this.positions.keySet.toList
          val tarr=jmap.get(this.rowcolumn).get.split(this.token)
         
          var j = 0
          if(!tarr.isEmpty){
            for(i <- 0 until tarr.length){
              output = output ++ Map(this.positions.get(keys(i%this.maxpos)) -> tarr(i))
              j += 1
              if(j ==  this.maxpos || (j+1 == tarr.length && output.size > 0)){
                output = output ++ Map(this.hashName -> jmap.get(this.hashKey).get)
                outlist = outlist ++ List(mapper.writeValueAsString(output)) 
                output = Map[String,String]()
                j = 0
              }
              
              
            }
            
            output=null
            
          }
        }
        
     }catch{
        case x:Throwable => log.error("Failed to Break: "+x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
      }
   
    outlist
  }
  
  
  def sendHandler(data:List[String]):Future[Boolean]=Future{
    var isInsert:Boolean = false
    log.info("Sending to DB")
    try{
       val mapper=new ObjectMapper() with ScalaObjectMapper
       mapper.registerModule(DefaultScalaModule)
       var value:String = null
       var table:String = null
       var sql:String = null
       var outlist: ArrayList[String] = new ArrayList[String]()
       var numvals:Int = 0
       
       if(data != null && !data.isEmpty){
         var fname:String = null
         table=this.targettable
         val keys = this.multiKeys.get(table).get
         sql = "INSERT INTO "+table+" ("
         value = "VALUES("
               
          var vals:Int = 0
               
          for(fname <- keys){
                 
             if(fname.compareTo("table") != 0 ){
                if(vals > 0){
                   value += ","
                   sql += ","
                 }
                 value += "?"
                 sql += fname
                 vals += 1
               }
           }
           numvals = vals
           sql += ")"
           value += ")"
           sql += " "+value
         data.foreach { json => 
           isInsert=true
           var jmap:Map[String,String]=mapper.readValue[Map[String,String]](json)
           if(table == null || outlist.size > this.commit_size){
             
               if(table != null && outlist.size > 0){
                 var keys=new ArrayList[String]()
                 this.multiKeys.get(table).get.foreach{x => keys.add(x)}
                 
                 this.getDAOTemplate.postJsonDataWithOrder(sql,outlist,keys)
                 outlist=new ArrayList[String]
               }
               
               
               if(!jmap.isEmpty){
                  if(notnull != null){
                    if(jmap.get(notnull.trim).get.orElse(null) != null && jmap.get(notnull.trim).get.length > 0){
                      outlist.add(json)
                    } 
                  }else{
                    outlist.add(json)
                  }
               }
           }else{
             if(notnull != null){
                    if(jmap.get(notnull.trim).get.orElse(null) != null && jmap.get(notnull.trim).get.length > 0){
                      outlist.add(json)
                    } 
              }else{
                    outlist.add(json)
              }
           }
         } 
       }
       
       if(outlist.size > 0){
         var keys=new ArrayList[String]()
         this.multiKeys.get(table).get.foreach{x => keys.add(x)}
         this.getDAOTemplate.postJsonDataWithOrder(sql,outlist,keys)
       }
       outlist=null
       sql = null
       value = null
    }catch{
      case x:Throwable =>{
        log.error("FAILED TO POST TO DB!\n"+x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
        isInsert = false
      }
    }
    isInsert
  }//sendHandler
  
  def getFromDB(query:String):Future[List[String]]=Future{
      this.getDAOTemplate.getJsonData(query).toList.map(x => x.asInstanceOf[String])
  }//getFromDB
  
  
  
  def checkTables()={
     if(this.multiKeys != null && this.targettable != null){
         
        this.multiKeys.keys.foreach { table => 
          val tarr=table.split('.')
          this.getDAOTemplate.execute("DROP TABLE IF EXISTS "+table+" CASCADE")
          
          var terms=""
          
          this.multiKeys.get(table).get.foreach { k => 
            if(!k.equals("table") && !k.contains("narrow")){
              terms = if(terms.length  == 0)k+" text" else terms+","+k+" text"
            }
          }
          
          if(terms.length > 0){
            this.getDAOTemplate.execute("DROP SEQUENCE IF EXISTS "+table.replace('.','_')+"_id_seq")
            this.getDAOTemplate.execute("CREATE TABLE IF NOT EXISTS "+table+" (id SERIAL,"+terms+", datestamp TIMESTAMP DEFAULT now())")
          }
        }
       
        if(this.index != null){
           this.index.keySet.foreach { name => 
               this.getDAOTemplate.execute("DROP INDEX IF EXISTS "+name)
               this.getDAOTemplate.execute("CREATE INDEX "+name+" ON "+this.targettable+" USING ("+this.index.get(name).mkString(",")+")")
           }
        }
       
     }else{
       throw new NullPointerException("Failed to Specify either MultiKeys or Target Table or Both")
     }
  }
  
  def run()={
    log.info("Starting Break @ "+Calendar.getInstance.getTime.toString())
    log.info("Target TAble IS "+this.targettable)
    var pulled:List[Future[Any]] = null
    var condition:String = null
    var numRecs:Int = 0
    var newRecs:Int = 0
    var chunksize = Math.ceil(this.pullsize/this.qnum).asInstanceOf[Int]
    var start = this.offset
    
    if(multiKeys == null){
      val keys = this.positions.values.toList ++ List(this.hashName)
      multiKeys = Map(this.targettable -> keys)
    }
    this.checkTables
    
    log.info("Performing Initial Parse")
    do{
      numRecs=newRecs
      
      pulled = List[Future[Any]]()
      
      for(conn <- 0 to qnum.asInstanceOf[Int]){
        condition = " WHERE " + this.idcolumn + " >= " + (start + (conn * chunksize)) + " AND " + this.idcolumn.asInstanceOf[String] + " < " + Integer.toString(start + (chunksize * (conn + 1)));
        if (extracondition != null) {
          condition += " " + extracondition.trim();
        }
        
        
        condition = select+" "+condition
        if(this.extracondition != null){
          condition +=" "+this.extracondition
        }
        
        pulled=pulled ++ List(this.getFromDB(condition).flatMap { x => this.BreakHandler(x)}.flatMap { x => this.sendHandler(x.filter(x=> x.length > 0))}) 
      }
      
      
      log.info("Waiting for Processes to Complete")
      var fails:Int = 0
      var successes:Int = 0
      val dur= if(this.termtime < 0) Duration.Inf else Duration(this.termtime,"millis")
      Await.ready(Future.sequence(pulled),dur).onComplete {
        case Success(x) =>{
            x.foreach { y =>{
                if(y ==false){
                  fails +=1
                }
            } }
        }
        case Failure(x) => log.error(x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
      }
      
            
      if(fails > 0){
        log.warn("Failures Exist In this Run!\n Failed Sets: "+String.valueOf(fails))
      }

      newRecs=this.getDAOTemplate.getCount(this.targettable)
      start+=commit_size
    }while(newRecs > numRecs)
     
      
    if(this.checkstring != null){
      log.info("Performing Final Sweep")
      var attempts:Int=0;
      do{
        /*find the table Name */
        numRecs=newRecs
      
        pulled=pulled ++ List(this.getFromDB(this.checkstring).flatMap { x => this.BreakHandler(x)}.flatMap { x => this.sendHandler(x)}) 
        
        
        log.info("Waiting for Processes to Complete")
        var fails:Int = 0
        var successes:Int = 0
        val dur= if(this.termtime < 0) Duration.Inf else Duration(this.termtime,"millis")
        Await.ready(Future.sequence(pulled),dur).onComplete {
          case Success(x)=>{
            x.foreach { y =>
                if(y == false){
                  fails+=1
                }
            }
          }
          case Failure(x) => log.error(x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
        }
        
              
        if(fails > 0){
          log.warn("Failures Exist In this Run!\n Failed Sets: "+String.valueOf(fails))
        }

        newRecs=this.getDAOTemplate.getCount(this.targettable)
      }while(newRecs > numRecs && attempts < numReattempts)
    }
    
    
    log.info("Completed Break @ "+Calendar.getInstance.getTime.toString())
  }
}
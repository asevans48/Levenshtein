package com.hygenics.scala

import com.google.common.escape.UnicodeEscaper

import org.jsoup.Jsoup
import org.jsoup.select.Elements

import com.hygenics.jdbc.jdbcconn

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import java.util.{Calendar,Date,Set,Collection,HashSet,ArrayList}
import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import javax.annotation.{Resource}

import org.springframework.beans.factory.annotation.Required

import com.hygenics.parser.getDAOTemplate
import org.springframework.context.annotation.{Bean}
import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.{Autowired}

import scala.beans._
import scala.collection.TraversableOnce
import scala.collection.JavaConversions._


import scala.util.matching.Regex

import org.springframework.beans.factory.annotation.Autowired;
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await,Future}
import scala.concurrent.duration._
import scala.util.{Success,Failure}
import org.slf4j.{LoggerFactory,Logger}
import com.hygenics.parser.Properties;

/**
 * A Scala version of the original ParseDispatcher. Scala
 * is used here to improve efficiency, speed, and even memory
 * use due to the improved multi-threading framework. 
 * 
 * Scala Advantages:
 * --parsing to different data types
 * --better framework for threading/multi-processing
 * --functional for faster additions to manipulate data with less lines of code
 * --less lines of code for easier understanding of components
 * 
 * Java Advantages:
 * 
 * --better networking for images ;etc.
 * --easier SQL access
 * --native file access (not really an advantage since Scala uses java filesystem)
 *  
 */
class ScalaParseDispatcher{
  
  val log:Logger=LoggerFactory.getLogger(this.getClass.getName)
  
  @Autowired
  var getDAOTemplate:getDAOTemplate=null
  
  @Required
  @BeanProperty
  var mainTable:String = null
  
  @BeanProperty
  var escapeUnicode : Boolean = false
  
  @BeanProperty
  var unicodeReplace : String = " "
  
  @BeanProperty
  var numReattempts:Int = 5
  
  
  @BeanProperty
  var multiKeys: Map[String,List[String]] = null
  
  @BeanProperty
  var termtime: Long = 5000L
  
  @BeanProperty
  var replace:String = ""
  
  @BeanProperty
  var hashName:String = "offenderhash"
  
  @BeanProperty 
  var reattempts:Int = 10

  @BeanProperty 
  var index:java.util.Map[String, java.util.Map[String,java.util.List[String]]] = null
  
  
  @BeanProperty 
  var test:Boolean = false

  @BeanProperty 
  var extracondition:String = _
  
  @BeanProperty 
  var notnull:String = _
  
  @BeanProperty 
  var pullsize:Int = 100
  
  @BeanProperty 
  var loops:Int = 0

  
  @BeanProperty 
  var qnum:Int = 5

  @BeanProperty 
  var schema:String = _

  @BeanProperty 
  var cannotcontain:String = _
  
  @BeanProperty 
  var mustcontain:String = _
  
  @BeanProperty 
  var replacementPattern:String = _
  
  @BeanProperty 
  var pullid:String = _
  
  @BeanProperty
  var escape :Boolean = false
  
  @BeanProperty 
  var select:String = _
  
  @BeanProperty 
  var column:String = _

  @BeanProperty 
  var checkstring:String = _

  @BeanProperty 
  var commit_size:Int = 100
  
  // these shouldn't be too large
  @BeanProperty 
  var singlepats:java.util.Map[String, String]= null
  
  
  @BeanProperty 
  var loopedpats:java.util.Map[String, java.util.Map[String, String]]= null

  @BeanProperty 
  var offset:String = null

  @BeanProperty
  var SPLITSIZE:Int = 500
  
  
  def parseAllTags(html:String,nkey:String,attrs:String):List[String]={
    var resList:List[String] = null
    
    if(nkey.toLowerCase.contains("soupid")){
      Jsoup.parse(html).getElementsByAttributeValue("id", attrs).map { x =>{  
        if(this.replacementPattern != null){
          x.toString.replaceAll(this.replacementPattern,this.replace).trim 
        }else{
          x.toString.trim
        }
        
      }}.toList
    }else if(nkey.toLowerCase.contains("souptag")){
      Jsoup.parse(html).getElementsByTag(attrs).map { x =>{  
        if(this.replacementPattern != null){
          x.toString.replaceAll(this.replacementPattern,this.replace).trim
        }else{
          x.toString.trim
        }
        
      }}.toList
    }else if(nkey.toLowerCase.contains("souptext")){
      Jsoup.parse(html).getElementsMatchingText(java.util.regex.Pattern.compile(attrs)).map { x =>{  
        if(this.replacementPattern != null){
          x.toString.replaceAll(this.replacementPattern,this.replace).trim
        }else{
          x.toString.trim
        }
        
      }}.toList
    }else if(nkey.toLowerCase.contains("soupvalue")){
      val tgarr=attrs.split("\\:")
      Jsoup.parse(html).getElementsByAttributeValue(tgarr(0),tgarr(1)).map { x =>{  
        if(this.replacementPattern != null){
          x.toString.replaceAll(this.replacementPattern,this.replace).trim
        }else{
          x.toString.trim
        }
        
      }}.toList
    }else if(nkey.toLowerCase.contains("soupmatchingvalue")){
      val tgarr=attrs.split("\\:")
      Jsoup.parse(html).getElementsByAttributeValueMatching(tgarr(0), java.util.regex.Pattern.compile(tgarr(1))).map { x =>{  
        if(this.replacementPattern != null){
          x.toString.replaceAll(this.replacementPattern,this.replace).trim
        }else{
          x.toString.trim
        }
        
      }}.toList
    }else if(nkey.toLowerCase.contains("soupclass")){
      Jsoup.parse(html).getElementsByClass(attrs).map { x => x.toString }.map { x =>{  
        if(this.replacementPattern != null){
          x.toString.replaceAll(this.replacementPattern,this.replace).trim
        }else{
          x.toString.trim
        }
        
      }}.toList
    }else if(nkey.toLowerCase.contains("soupattrvalue")){
      val tgarr=attrs.split("\\:")
      Jsoup.parse(html).getElementsByAttributeValue(tgarr(0),tgarr(1)).toList.map { x => x.attr(tgarr(2)) } 
    }else{
      null
    }
  }
  
  def parseSoup(html:String,nkey:String,attrs:String):String={
    var v : String = null
    try{
      if(nkey.toLowerCase.contains("soupid")){
        v=Jsoup.parse(html).getElementById(attrs).text
      }else if(nkey.toLowerCase.contains("souptag")){
        val tgarr=attrs.split("\\:")
        v=Jsoup.parse(html).getElementsByTag(tgarr(0)).get(Integer.valueOf(tgarr(1))).text
      }else if(nkey.toLowerCase.contains("souptext")){
        v=Jsoup.parse(html).getElementsMatchingText(java.util.regex.Pattern.compile(attrs)).get(0).text
      }else if(nkey.toLowerCase.contains("soupvalue")){
        val tgarr=attrs.split("\\:")
        v=Jsoup.parse(html).getElementsByAttributeValue(tgarr(0),tgarr(1)).get(0).text
      }else if(nkey.toLowerCase.contains("soupmatchingvalue")){
        val tgarr=attrs.split("\\:")
        v=Jsoup.parse(html).getElementsByAttributeValueMatching(tgarr(0), java.util.regex.Pattern.compile(tgarr(1))).get(0).text
      }else if(nkey.toLowerCase.contains("soupclass")){
        v=Jsoup.parse(html).getElementsByClass(attrs).get(0).text
      }else if(nkey.toLowerCase.contains("soupattrvalue")){
        val tgarr=attrs.split("\\:")
        v=Jsoup.parse(html).getElementsByAttributeValue(tgarr(0), tgarr(1)).get(Integer.valueOf(tgarr(2))).attr(tgarr(3))
      }
    }catch{
      case t :Throwable => t.getMessage 
    }
    v
  }
  
  /**
   * Parses Pages with Singly nested Map patterns
   */
  def parseSingle(json:String)=Future[List[String]]{
    var rval:String = null
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    try{
      if(this.singlepats != null){
       
        var jmap:Map[String,String]=mapper.readValue[Map[String,String]](json)
        var html:String = jmap.get(this.column).get
        val hash:String = jmap.get(this.hashName).get
        val table:String = this.singlepats.get("table") 
        jmap=null
        
        if(this.singlepats.containsKey("narrow")){
          html=this.singlepats.get("narrow").r.findFirstIn(html).getOrElse(html)
        }else if(this.singlepats.containsKey("narrowSoup")){
          val ntag=this.singlepats.get("narrowSoup").split("::")
          html=this.parseSoup(html,ntag(0),ntag(1))
        }
        
        if(this.singlepats.containsKey("additionalNarrow")){
          html=this.singlepats.get("additionalNarrow").r.findFirstIn(html).getOrElse(html)
        }else if(this.singlepats.containsKey("additionalNarrowSoup")){
          val ntag=this.singlepats.get("additionalNarrowSoup").split("::")
          html=this.parseSoup(html,ntag(0),ntag(1))
        }
    
        
        var result:Map[String,String] = Map[String,String]()
        for(k:String <- this.singlepats.keySet){
            if(!(k.contains("narrow") || k.equals("table"))){
              //get data
              if(!k.toLowerCase.contains("soup")){
                  //straight regular expression matching
                  val mp = (k->StringEscapeUtils.escapeJson(this.singlepats.get(k).r.findFirstIn(html).getOrElse("")))
                  result = result + mp
              }else{
                  //parse with JSoup
                  try{
                    val nkey:String = k.replaceAll("(?i)Soup[^:]+","")
                    var v = this.parseSoup(html, k,this.singlepats.get(k))
                    
                    if(escape){
                        v =StringEscapeUtils.escapeJava(new String(v.getBytes,System.getProperty("file.encoding")))
                    }
                      
                    if(escapeUnicode){
                      v = v.replaceAll("[^\\u0020-\\u007FA-Za-z0-9\\.\\,\\;\\-\\?\\!\\$\\*\\:\\/\\)\\(\\@\\#\\&\\`\\~\\{\\}\\[\\]\\<\\\\]+", unicodeReplace)
                    }
                    
                    val mp = if(v  == null)(nkey -> "") else (nkey-> StringEscapeUtils.escapeJson(v))
                    result = result + mp
                  }catch{
                    case x:NullPointerException =>{
                      log.info("No Match: "+x.getMessage)
                      result = result + (k.replaceAll("(?mis)Soup[^:]+","") -> "")
                    }
                    case t:Throwable => log.error("Error in Parsing: "+t.getMessage+"\n"+ExceptionUtils.getStackTrace(t))
                  }
              }
            }
        }
        
        
        if(result.size > 0){
          result=result.mapValues { x => 
              if(this.replacementPattern != null) x.replaceAll(this.replacementPattern,this.replace).trim else x.trim
           }
          result = result ++ Map(("table" -> table))
          result = result ++ Map((this.hashName -> hash))
          rval = mapper.writeValueAsString(result)
          result=null
        }
      }
    }catch{
      case x:Throwable =>{
         rval=""
         log.error(x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
      }
    }
    List(rval)
  }//parsepage
  
  /**
   * Parses Pages with Looped Patterns (multi-Mapped Patterns)
   */
  def parseLooped(json:String)=Future[List[String]]{
    var jsons = List[String]()
    try{
      if(this.loopedpats != null){
        val mapper=new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        var jmap:Map[String,String]=mapper.readValue[Map[String,String]](json)
        val uhtml:String = jmap.get(this.column).get
        val hash:String = jmap.get(this.hashName).get
        jmap=null
        
        for(table <- this.loopedpats.keySet()){
          try{
            val tmap=this.loopedpats.get(table)
            var html:String=uhtml 
            
            if(tmap.containsKey("narrow")){
              html=tmap.get("narrow").r.findFirstIn(html).getOrElse(html)
            }else if(tmap.containsKey("narrowSoup")){
              val ntag=tmap.get("narrowSoup").split("::")
              html=this.parseSoup(html,ntag(0),ntag(1))
            }
            
            if(tmap.containsKey("additionalNarrow")){
              html=tmap.get("additionalNarrow").r.findFirstIn(html).getOrElse(html)
            }else if(tmap.containsKey("additionalNarrowSoup")){
              val ntag=tmap.get("additionalNarrowSoup").split("::")
              html=this.parseSoup(html,ntag(0),ntag(1))
            }
            
            var result:Map[String,String] = Map[String,String]()
            for(k:String <- tmap.keySet){
              try{
                if(!(k.contains("narrow") || k.equals("table"))){
                  //get data
                  if(!k.contains("Soup")){
                      //straight regular expression matching
                      var m= tmap.get(k).r.findAllIn(html).map { x =>
                        if(this.replacementPattern != null) x.replaceAll(this.replacementPattern, this.replace).trim else x.trim
                      }.mkString("|")
                      
                      if(escape){
                        m = StringEscapeUtils.escapeJava(new String(m.getBytes,System.getProperty("file.encoding")))
                      }
                      
                      if(escapeUnicode){
                        m = m.replaceAll("[^\\u0020-\\u007FA-Za-z0-9\\.\\,\\;\\-\\?\\!\\$\\*\\:\\/\\)\\(\\@\\#\\&\\`\\~\\{\\}\\[\\]\\<\\\\]+", unicodeReplace)
                      }
                      
                      if(m != null && m.length > 0){
                        
                        result = result ++ Map((k ->StringEscapeUtils.escapeJson(m)))
                      }
                  }else{
                      //parse with JSoup
                      val nkey:String ="""(?mis)Soup[^:]+""".r.findFirstIn(k).get
                      var m=this.parseAllTags(html, nkey,tmap.get(k)).mkString("|")
                      
                      if(escape){
                        m = StringEscapeUtils.escapeJava(new String(m.getBytes,System.getProperty("file.encoding")))
                      }
                      
                      if(escapeUnicode){
                        m = m.replaceAll("[^\\u0020-\\u007FA-Za-z0-9\\.\\,\\;\\-\\?\\!\\$\\*\\:\\/\\)\\(\\@\\#\\&\\`\\~\\{\\}\\[\\]\\<\\\\]+", unicodeReplace)
                      }
                      
                      
                      if(m != null && m.length > 0){
                        result = result ++ Map((k.replaceAll("(?mis)Soup.*","") -> StringEscapeUtils.escapeJson(m)))
                      }
                  }
                }
              }catch{
                case x:Throwable => log.error("Failed to Parse: "+x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
              }
            }
            if(result.size > 0){
              result = result ++ Map(("table" -> table))
              result = result ++ Map((this.hashName -> hash))
              jsons = jsons ++ List(mapper.writeValueAsString(result))
              result=Map[String,String]()
            }
          }catch{
            case x:Throwable => log.info("Failed to Parse for table "+table+": "+x.getMessage+"\n"+ExceptionUtils.getStackTrace(x))
          }  
        }
      }
    }catch{
      case _:Throwable =>{
        jsons=List("")
      }
    }
    jsons
  }//parseMultiPage 
  
  def parseHandler(jsons:List[String]):Future[List[String]]=Future{
    //prepare
    var futs:List[Future[List[String]]] = List[Future[List[String]]]()
    val mapper=new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    jsons.foreach { json =>{
      var jmap:Map[String,String]=mapper.readValue[Map[String,String]](json)
  
      if(test){
        log.info("------------------------\n-------------------------------------\nHTML\n-------------------------------------\n\n")
        log.info(jmap.get(this.column).get.asInstanceOf[String])
      }
      
      if(this.singlepats != null && this.singlepats.size() > 0 ){
          
          futs = futs ++ List(this.parseSingle(json))
      }
      
      if(this.loopedpats != null && this.loopedpats.size > 0){
        futs = futs ++ List(this.parseLooped(json))
      }
    }}
    val r=Await.result(Future.sequence(futs),Duration(this.termtime,"millis")).flatten
    futs=null
    r.filter { x => (x != null && x.size > 0) }
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
       
       if(data != null && data.size > 0){
         data.foreach { json => 
           isInsert=true
           var jmap:Map[String,String]=mapper.readValue[Map[String,String]](json)
           if(table == null || !table.equals(jmap.get("table").get) || outlist.size > this.commit_size){
             
               if(table != null && outlist.size > 0){
                 var keys=new ArrayList[String]()
                 this.multiKeys.get(table).get.foreach{x => if(!x.equals("table")) keys.add(x)}
                 this.getDAOTemplate.postJsonDataWithOrder(sql,outlist,keys)
                 outlist=new ArrayList[String]
               }
               
               var fname:String = null
               table = jmap.get("table").get
               val keys = this.multiKeys.get(table).get.map { x => x.replaceAll("(?mis)Soup.*","") }
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
               
               if(json.trim.compareTo("No Data") != 0){
                  if(notnull != null){
                    if(jmap.get(notnull.trim).get.orElse(null) != null && jmap.get(notnull.trim).get.length > 0){
                      outlist.add(json)
                    } 
                  }else{
                    outlist.add(json)
                  }
               }
           }else if(table != null && jmap.get("table").get.equals(table)){
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
         this.multiKeys.get(table).get.foreach{x => keys.add(x.replaceAll("(?mis)Soup.*",""))}
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
      this.getDAOTemplate.getJsonData(Properties.getProperty(query)).toList.map(x => x.asInstanceOf[String])
  }//getFromDB
  
  def createTables()={
      if(this.singlepats != null){
          val table=Properties.getProperty(this.singlepats.get("table"))
          val tarr=table.split('.')
          this.getDAOTemplate.execute("DROP TABLE IF EXISTS "+table+" CASCADE")
          
          var terms=""
          
          for(k:String <- this.multiKeys.get(table).get){
            if(!k.equals("table") && !k.contains("narrow")){
              terms = if(terms.length  == 0)k+" text" else terms+","+k+" text"
            }
          }
          
          if(terms.length > 0){
            this.getDAOTemplate.execute("DROP SEQUENCE IF EXISTS "+table.replace('.','_')+"_id_seq")
            this.getDAOTemplate.execute("CREATE TABLE IF NOT EXISTS "+table+" (id SERIAL,"+terms+", datestamp TIMESTAMP DEFAULT now())")
          }
      }
      
      if(this.loopedpats != null){
        for(table <- this.loopedpats.keySet){
            val tarr=table.asInstanceOf[String].split('.')
            this.getDAOTemplate.execute("DROP TABLE IF EXISTS "+table+" CASCADE")
            
            var terms=""
            
            for(k:String <- this.multiKeys.get(table).get){
              if(!(k.equals("narrow") || k.equals("additionalrenarrow"))){
                  terms = if(terms.length  == 0)k+" text" else terms+","+k+" text"
              }
            }
            
            if(terms.length > 0){
               this.getDAOTemplate.execute("DROP SEQUENCE IF EXISTS "+table.replace('.','_')+"_id_seq")
               this.getDAOTemplate.execute("CREATE TABLE IF NOT EXISTS "+table+" (id SERIAL,"+terms+", datestamp TIMESTAMP DEFAULT now())")
            }
        }
        
        if(this.index != null){
           this.index.keySet.foreach { table => 
               val name:String = this.index.get(table).keySet.iterator.next //will fail on empty
               this.getDAOTemplate.execute("DROP INDEX IF EXISTS "+name)
               this.getDAOTemplate.execute("CREATE INDEX "+name+" ON "+table+" USING ("+this.index.get(table).get(name).mkString(",")+")")
           }
        }
        
      }
  }//createTables
  
  def getOffsetFromSql(q:String):Int={
    val data = this.getDAOTemplate.getAll(q)
    if(data != null && data.size() > 0){
      val mapper=new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val jmap:Map[String,String]=mapper.readValue[Map[String,String]](data.get(0))
      val k = jmap.keySet.iterator.next()
      return jmap.get(k).get.toInt
    }
    0
  }
  
  def run()={
    log.info("Starting Parse @ "+Calendar.getInstance.getTime.toString())
    
    //check offset
    var start:Int = 1
    if(this.offset != null){
      this.offset = Properties.getProperty(this.offset)
      
      if(this.offset.trim.startsWith("SQL:")){
        start = this.getOffsetFromSql(this.offset.replace("SQL:","").trim)
      }else{
        start = this.offset.trim.toInt
      }
      
    }
    
    //generate keys
    if(multiKeys == null){
      multiKeys = Map[String,List[String]]()
    }
    if(this.singlepats != null && !this.multiKeys.containsKey(this.singlepats.get("table"))){
      val loopkeys = this.singlepats.keySet.toList ++ List(this.hashName)
      this.multiKeys = this.multiKeys ++ Map(this.singlepats.get("table") -> loopkeys.map { x => x.replaceAll("(?mis)Soup.*","") })
    }
   
    if(this.loopedpats != null){
      loopedpats.keySet.foreach { k =>
        if(!this.multiKeys.contains(k)){
          var loopkeys=List[String]()
          val map=loopedpats.get(k)
          map.keySet.foreach { x => if(!x.toLowerCase.contains("narrow"))loopkeys = loopkeys ++ List(x)}
          map.remove("table")
          loopkeys= loopkeys.map { x => x.replaceAll("(?mis)Soup.*","")} ++ List(this.hashName)
          multiKeys= multiKeys ++ Map(k -> loopkeys)
        }
      }
    }
    
    val t =  Calendar.getInstance.getTimeInMillis
    var pid:Int = 0
    var id:Int = 0
    var checkattempts:Int =0 
    var add:String = null
    
    var chunksize = Math.ceil(this.pullsize/this.qnum).asInstanceOf[Int]
    var condition:String = null
    var pulled:List[Future[Any]] = null
    log.info("Checking Tables")
    this.createTables
    
    var newRecs:Int = 0
    var numRecs:Int = 0
    
    this.extracondition = Properties.getProperty(this.extracondition)
    this.select = Properties.getProperty(this.select)
    this.schema = Properties.getProperty(this.schema)
    this.column = Properties.getProperty(this.column)
    
    log.info("Performing Initial Parse")
    do{
      numRecs=newRecs
      pulled=null
      pulled = List[Future[Any]]()
      
      for(conn <- 0 to qnum-1){
        condition = " WHERE " + pullid + " >= " + (start + (conn * chunksize)) + " AND " + pullid.asInstanceOf[String] + " < " + Integer.toString(start + (chunksize * (conn + 1)));

        if (extracondition != null) {
          condition += " " + extracondition.trim();
        }
        
        
        condition = select+" "+condition
        if(this.extracondition != null){
          condition +=" "+this.extracondition
        }
        
        pulled = pulled ++ List(this.getFromDB(condition).flatMap { x => this.parseHandler(x)}.flatMap { x => this.sendHandler(x)}) 
      }
      
      
      log.info("Waiting for Processes to Complete")
      var fails:Int = 0
      var successes:Int = 0
      val dur= if(this.termtime < 0) Duration.Inf else Duration(this.termtime*qnum*5,"millis")
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

      newRecs=this.getDAOTemplate.getCount(this.mainTable)
      start+=commit_size
    }while(newRecs > numRecs)
     
    if(this.checkstring != null){
      log.info("Performing Final Sweep")
      val table=for(m <- """FROM\s+([^\s]+)\s+""".r findFirstMatchIn this.select) yield m group 1
      var attempts:Int=0;
      do{
        /*find the table Name */
        numRecs=newRecs
        pulled=List[Future[Any]]()
        pulled=pulled ++ List(this.getFromDB(this.checkstring).flatMap { x => this.parseHandler(x)}.flatMap { x => this.sendHandler(x)}) 
        
        
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

        newRecs=this.getDAOTemplate.getCount(this.mainTable)
      }while(newRecs > numRecs && attempts < numReattempts)
    }
    pulled = null
    log.info("Completed Parse @ "+Calendar.getInstance.getTime.toString())
    log.info("Time Elapsed: "+((Calendar.getInstance.getTimeInMillis-t)/1000))
  }//run
}
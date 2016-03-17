package com.hygenics.scala

import scala.collection.JavaConversions._
import java.util.ArrayList

import scala.collection.convert.decorateAsScala._

import scala.concurrent.duration.Duration
import scala.concurrent.{Future,Await}

import scala.concurrent.ExecutionContext.Implicits.global

import better.files._

import scala.annotation.tailrec
import scala.collection.mutable.HashSet

import sbt.IO
import java.io.File
import java.nio.file.Files

import scala.collection.concurrent.{Map => cMap}
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang3.exception.ExceptionUtils

import org.slf4j.{Logger,LoggerFactory}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Uses the apparently significant power of Scalas sets and other features to perform file Cleaning Operations.
 * This object is good for a variety of tasks such as cleaning out files missing from a database.
 */
class FileOps {
  private var log:Logger = LoggerFactory.getLogger(this.getClass.getName)
  var fSet:Set[File] = _
  
  def delFiles(file:better.files.File)=Future{
    file.delete(false)
  }
  
  /**
   * Upload files to the database from a linked directory. Agains the process in the FJP makes a difference.
   * The map column is treated as unique.
   * 
   * @param		jsons										A list of json strings pulled from the database.
   * @param		hashCol									The column containing the hash for the pull.
   * @param		mapCol									The column to be mappd to.
   * @param		directory								The directory to map from. 
   * @param   replacementPattern			The pattern to replace in matching to the array
   * @param		{Boolean}{recursive}		Whether or not to look recursively through a directory (default true)	
   * @param		table										The table to attach to the data if present.
   * 
   * @return	a java ArrayList[String] of jsons containing the hash attached to hashCol and mapping attached to mapCol	
   */
  def getLinkedDirectoryIntersection(matches:String,directory:String,jsons:java.util.ArrayList[String],hashCol:String,mapCol:String,replacementPattern:String = null, recursive:Boolean = true, table:String = null,pathName:String = null,delFiles:Boolean = false):ArrayList[String]={
      fSet= Set[File]()
     
      val mapper=new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      var map:cMap[String,String] = new ConcurrentHashMap().asScala
      log.info("Getting Map From Data")
      jsons.foreach { x =>{
          val jmap = mapper.readValue[Map[String,String]](x)
          val k = if(replacementPattern != null) jmap.get(mapCol).get.replaceAll(replacementPattern,"") else jmap.get(mapCol).get
          val v = jmap.get(hashCol).get
          map.put(k, v)
      }}
      
      
      def getTup(f : better.files.File):Future[Map[String,String]] = Future{
        var res:Map[String,String] = null
        val k = f.name.replaceAll(replacementPattern,"")
        try{
          if(map.contains(k)){
             res=Map(f.name -> map.get(k).get)
           }else if(delFiles){
              f.delete(false)
           }
        }catch{
          case e:java.io.IOException => log.error("IO EXCEPTION: "+e.getMessage)
          case t:Throwable => log.error("OTHER ISSUE: "+t.getMessage)
        }
         res
      }
      
     
      def getFiles(pth:File):Future[Map[String,String]]=Future{
          log.info(pth.getAbsolutePath)
          var results:Map[String,String]=Map[String,String]()
          val dir = better.files.File(pth.getAbsolutePath)
          val files:better.files.Files =  dir.glob(matches, syntax = "regex", ignoreIOExceptions = false)
          val res = Await.result(Future.traverse(files.filter { x => x.size > 0 })(getTup(_)),Duration.Inf).filter( x => x != null)
          log.info("Building Results")
          res.foreach( x => results = results ++ x)
          results
      }
      
      log.info("Getting Data")
      var results: ArrayList[String] = new ArrayList[String]()
      val res = Await.result(getFiles(new java.io.File(directory)),Duration.Inf)
      
      log.info("Prepping Results")
      res.foreach(x => {
          var mp:Map[String,String] = Map[String,String]()
          mp = mp + (mapCol -> x._1.replaceAll("\\..*",""), hashCol -> x._2)
          
          if(table != null){
            mp = mp + ("table" -> table)
          }
          
          if(pathName != null){
            mp = mp + (pathName ->  x._1)
          }
          
          results.add(mapper.writeValueAsString(mp))
      })
      log.info("Found "+results.size()+" results")
      results
  }
  
  /**
   * Clean a directory (recursively) of empty files or all files that do not connect to a data in an array.
   * Speed bosts from a single function in the fjp are actually quit significant. File locks prevent
   * the deletion of multiple files at once on my operating systems, making the the process of multi-threading
   * deletions slower than doing such an activity in a single thread.
   * 
   * @param		dir										The directory to use in cleaning.
   * @param		arr										The array containing the values to match on.
   * @param		avoidanceString				A string regex pattern to use specifying files to skip.
   * @param		emptyOnly							A file stating that only empty files should be deleted.
   * @param		replacementPattern		A pattern to replace to match against the array.
   * @param		recursive						  A boolean stating whether to recursively search a directory.
   */
  def cleanDirectory(dir:File,arr:java.util.List[String],avoidanceString:String,emptyOnly:Boolean = false,replacementPattern:String = null,recursive:Boolean = true)={
    log.info(if(avoidanceString != null)"Avoiding: "+avoidanceString else "Not Avoiding Anything")
    log.info(if(replacementPattern != null)"Replacing With: "+replacementPattern else "Not Replacing Anything")
    
    fSet= Set[File]()
    
    
    var colSet:HashSet[String] = HashSet.apply(arr:_*) //the lookup table
    
    if(replacementPattern != null){
      colSet = colSet.map { x => x.replaceAll(replacementPattern,"")}
    }
    
    //remove files
    def removal(dirPath:File,avoidanceString:String,replacementPattern:String):Future[Set[better.files.File]]=Future{
      var iSet:Set[better.files.File] = Set[better.files.File]()
      val dir = better.files.File(dirPath.getAbsolutePath)
      val files:better.files.Files =  dir.list
      
      files.foreach({ file => 
          if(file.isDirectory){
             if(recursive && (avoidanceString == null || file.name.matches(avoidanceString) == false)){
               try{
                 
                  val res = Await.result(removal(file.path.toJava,avoidanceString,replacementPattern),Duration(450000,"millis"))
                  iSet = iSet ++ res
               }catch{
                 case e:java.io.FileNotFoundException => log.error("FILE NOT FOUND:\n"+e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
                 case e:java.io.IOException => log.error("IO EXCEPTION:\n"+e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
                 case e:java.io.IOError => log.error("IO ERROR:\n"+e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
                 case e:Throwable => log.error("Other Error Occurred:\n"+e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
               }
             }
          }else{
            val fname =if(replacementPattern != null) file.name.replaceAll(replacementPattern,"") else file.name
            if(!colSet.contains(fname) && (avoidanceString == null || file.name.matches(avoidanceString) == false)){ //guessing a lookup table is faster
              iSet = iSet + file
            }
          }
      })
      iSet
    }
    
    
    val dSet = Await.result(removal(dir,avoidanceString,replacementPattern),Duration.Inf)
    
    if(dSet.size > 0){
      log.info(dSet.size.toString)
      Await.ready(Future.traverse(dSet)(delFiles(_)),Duration.Inf)
      
    }
  }
  
  def run(dir:File,arr:java.util.List[String],avoidanceString:String,emptyOnly:Boolean = false,replacementPattern:String = null,recursive:Boolean = true)={
    cleanDirectory(dir, arr, avoidanceString, emptyOnly, replacementPattern,recursive)
  }
}
package com.hygenics.scala


import scala.beans._
import java.io.{File,FileNotFoundException,IOException}
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await,Future}
import scala.concurrent.duration._
import scala.util.{Success,Failure}
import org.slf4j.{LoggerFactory,Logger}
import org.apache.commons.lang3.exception.ExceptionUtils
import sbt.IO
import scala.util.matching.Regex

/**
 * A class that is Bean Ready and will take in a source and target folder and copy over files from that directory.
 * Non-recursive or recursive. This class will not zip it is an extension of the zip capability, allowing for files
 * to only be moved. For zip capability, use the Zip Bean attached to the Archiver Class.
 * 
 * @author aevans
 */
class MoveFile {
  
  val log:Logger = LoggerFactory.getLogger(this.getClass.getName)
  @BeanProperty
  var source:String = _
  
  @BeanProperty
  var target:String = _
  
  @BeanProperty
  var termtime:Long = 120000L
  
  @BeanProperty
  var recursive:Boolean = false
  
  @BeanProperty
  var ignore: String = "(?mis).zip|.jpg|.jpeg|archive"
  
  @BeanProperty
  var preserve:Boolean = true //basically, copy or move
  
  @BeanProperty 
  var createDir:Boolean = true //if the directory should be created
  
  def copy(file:File,target:String)=Future{
    try{ 
       val f:File = new File(target)
       if(!f.exists){
         f.mkdirs
       }
       if(this.ignore.r.findAllMatchIn(file.getAbsolutePath).size == 0){
         if(this.preserve) IO.copyFile(file,new File(f,file.getName)) else IO.move(file,new File(f,file.getName))
       }
    }catch{
      case x:FileNotFoundException => log.error("FILE MISSING: "+x.getMessage+"\n"+ExceptionUtils.getMessage(x))
      case x:IOException => log.error("IO EXCEPTION: "+x.getMessage+"\n"+ExceptionUtils.getMessage(x))
      case x:Throwable => log.error("GENERAL COPY EXCEPtION: "+x.getMessage+"\n"+ExceptionUtils.getMessage(x))
    }
  }
 
  def getFiles(source:File):Array[File]={
    val files=IO.listFiles(source)
    for(pd <- files){
      if(pd.isDirectory) files ++ this.getFiles(pd)
    }
    files
  }
  
  def getFiles():Array[File]={
     var files:List[String]=List[String]()
     val tfiles=IO.listFiles(new File(this.source))
     
     if(this.recursive){
       for(pd <- tfiles){
         if(pd.isDirectory) tfiles ++ this.getFiles(pd)
       }
     }
     
     tfiles.filter { x => !x.isDirectory }
  }
  
  def run()={
    try{
      
      if(createDir){
        val targetDir = new File(target)
        
        if(!targetDir.exists){
          IO.createDirectory(targetDir)
        }
      }
      
     Await.ready(Future.traverse(this.getFiles().toList)(x => this.copy(x,this.target)),Duration.apply(this.termtime, "millis"))
    }catch{
      case x:Throwable => log.error("Failure in Move Files: "+x.getMessage+"\n"+ExceptionUtils.getMessage(x))
    }
  }
}
package com.hygenics.scala

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import au.com.bytecode.opencsv.CSVWriter
import java.io.FileWriter
import scala.beans._
import scala.collection.JavaConversions._
import org.slf4j.{LoggerFactory,Logger}

/**
 * A scala class that can be used for notification on completion of a Parser/QA/ETL process. 
 * Scala has some advantages here, mainly the easy ability to write and implement parsers.
 */
class Notification(@BeanProperty var source:String = null,@BeanProperty var notificationPath:String = null){
   
   val log:Logger = LoggerFactory.getLogger(this.getClass.getName)
  

    def run()={
         log.info("Notifying")
         if(source != null && notificationPath != null){
           val writer:CSVWriter=new CSVWriter(new FileWriter(notificationPath,true),',')
           val entry=Array(source,new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date()), java.lang.Long.toString(Calendar.getInstance.getTimeInMillis))
           writer.writeAll(List(entry))
           writer.close()
         } 
         log.info("Done Notifying")
    }
}
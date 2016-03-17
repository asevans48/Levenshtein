package com.hygenics.scala

import scala.util.matching.Regex
import scala.collection.JavaConversions._

class RegexAPI {
  var pattern : String = _
  
  def setPattern(pattern : String)={
    this.pattern = pattern
  }
  
  def multi_regex(page : String):Array[String]={
    this.pattern.r.findAllMatchIn(page).map { x => x.group(0) }.toArray
  }
  
  def single_regex(page : String):String={
    this.pattern.r.findFirstIn(page).getOrElse(null)
  }
}
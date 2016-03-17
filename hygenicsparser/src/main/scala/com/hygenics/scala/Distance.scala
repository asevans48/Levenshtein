package com.hygenics.scala

import com.hygenics.distance.Levenshtein
import com.hygenics.parser.MainApp
import org.slf4j.{Logger,LoggerFactory}

/**
 * A class that provides API access to distance algorithms.
 */
object Distance {
  val log:Logger = LoggerFactory.getLogger(classOf[MainApp])
  
  /**
   * Static method for acquiring distances of different types.
   * 
   * Currently Supported Distance Types are
   * 		-Levenshtein
   * 		-java based compareTo
   * 
   * Future methods may include metaphone but most of the input is text which works fairly well with 
   * Levenshtein.
   * 
   * @param		distType		Type of Distances
   * @param 	worda				The first word to be compared
   * @param		wordb				The second word to be compared
   * @return	The distance as a Double
   */
  def getDistance(distType:String,worda:String,wordb:String):Double={
    distType match{
      case "levenshtein" => Levenshtein.getLevenshteinDistance(worda, wordb)
      case _ =>{
        log.warn("No Matching distance algorithm discovered. Using CompareTo")
        return worda.compareTo(wordb)
      }
    }
  }
  
}
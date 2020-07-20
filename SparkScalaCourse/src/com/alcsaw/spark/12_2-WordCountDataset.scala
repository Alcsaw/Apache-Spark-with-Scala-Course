package com.alcsaw.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountDataset{
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("WordCountDataset")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
    val words = spark.sparkContext.textFile("../SparkScala3/book.txt")flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase and remove not interesting words
    val filteredWords = words.map(x => x.toLowerCase()).filter((x: String) => (x != "the" || x != "of" || x != "a" || x != "an" || x != "for" || x != "and" || x != "on" || x != "do" || x != "don" || x != "does" || x != "doesn" || x != "ve"))
    
    // Convert to a DataSet
    import spark.implicits._
    val wordsDS = filteredWords.toDS() 
    
    wordsDS.show(5)
    
    wordsDS.map(x => x.toLowerCase())    
    
    // count words
    val wordCount = wordsDS.groupBy("value").count().orderBy(desc("count"))
    
    wordCount.show()
    
    // Stop the session
    spark.stop()
  }
  
}

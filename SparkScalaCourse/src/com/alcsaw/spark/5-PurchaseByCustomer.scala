package com.alcsaw.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchaseByCustomer {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val customerID = fields(0)
    val amountSpent = fields(2).toFloat
    (customerID, amountSpent)
  }
  
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    // Load CSV file
    val input = sc.textFile("../SparkScala3/customer-orders.csv")
    
    // get the CustomerID and amountSpent
    val parsedLines = input.map(parseLine)
    
    // Reduce by customerID summing the amountSpent in each purchase
    val customerExpenses = parsedLines.reduceByKey( (x, y) => x + y )

    // Flip (customerID, amountSpent) tuples to (amountSpent, customerID) and then sort by key (the amountSpent)
    val customerExpensesSorted = customerExpenses.map( x => (x._2, x._1) ).sortByKey()
    
    // Collect, format, and print the results
    //val results = customerExpenses.collect()
    
    for (result <- customerExpensesSorted) {
       val customerID = result._2
       val amountSpent = result._1
       val formattedPurchase= """R$ """ + f"$amountSpent%.2f"
       println(s"$customerID purchased a total of $formattedPurchase") 
    }
  }
}
  
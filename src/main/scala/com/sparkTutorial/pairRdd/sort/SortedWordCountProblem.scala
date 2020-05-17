package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val cleanedLines = lines.filter(line => !line.isEmpty)

    val rdd = cleanedLines.flatMap(word => word.split(" "))
    val counted = rdd.map(word => (word, 1))
    val groupRdd = counted.reduceByKey((x, y) => x + y)
    val invertered = groupRdd.map(x => (x._2, x._1))
    val sorted = invertered.sortByKey(false)

    sorted.foreach(value => println(s"${value._2} - ${value._1}"))
  }

}


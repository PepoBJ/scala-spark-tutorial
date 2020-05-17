package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airpots").setMaster("local[*]")
    var sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsLatitude = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat >= 40)

    val airportsAndLatitude = airportsLatitude.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsAndLatitude.saveAsTextFile("out/airport_with_latitude_more_than_40.text")
  }
}

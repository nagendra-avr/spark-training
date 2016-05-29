package com.scala.training.spark.sql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


/**
 * Created by Nagendra Amalakanta on 5/28/16.
 */
object ScalaSqlJSONRead {

  def main(args: Array[String]) {

    if(args.length < 1) {
      println("Usage ScalaSqlJSONRead : <Json File>")
    }
    //Suppress Spark output
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sconf = new SparkConf();
    sconf.setAppName("Spark-Sql-Training")
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Returns DataFrame
    val jsondata = sqlContext.read.json(args(0));

    jsondata.printSchema();

    //Default 20 rows
    jsondata.show();

  }
}

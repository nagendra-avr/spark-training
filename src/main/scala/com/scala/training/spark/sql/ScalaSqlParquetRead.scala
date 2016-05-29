package com.scala.training.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Nagendra Amalakanta on 5/29/16.
 */
object ScalaSqlParquetRead {

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
    val parquetdf = sqlContext.read.parquet(args(0));

    parquetdf.printSchema();

    //select fields
    parquetdf.select("id").show()

    //select multiple fields
    parquetdf.select(parquetdf("id"),parquetdf("title")).show()
   // parquetdf.select($"id",$"title",$"text").show()

    //filter fields
    parquetdf.filter(parquetdf("id").equalTo("12358080")).show()
    //parquetdf.filter($"id === 12358080" ).show()

    //Aggregations
    //Group By id
     parquetdf.groupBy(parquetdf("id")).count().show()

    parquetdf.filter(parquetdf("id").equalTo("12358080") || parquetdf("id").equalTo("12358080")).show()
    parquetdf.where(parquetdf("id").equalTo("12358080") ||parquetdf("id").equalTo("12358080")).show()

    //parquetdf.filter($"id"==="12140913" || $"id"==="14315350").show()
    //parquetdf.where($"id"==="12140913" || $"id"==="14315350").show()
    //parquetdf.where(wikidata("id")==="12140913" || wikidata("id")==="14315350").show()

    //Default 20 rows
    parquetdf.show();

  }

}

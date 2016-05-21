package com.scala.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Nagendra Amalakanta on 5/21/16.
 */
object TextFileStreamWordCount {

  def main(args: Array[String]) {

    if(args.length < 1) {
      System.err.println("Usage: TextFileStreamWordCount <directoryName>")
      System.exit(1)
    }

    val sparkconf = new SparkConf().setAppName("TextFileStreamWordCountScala");
    val ssc = new StreamingContext(sparkconf,Seconds(10));

    val lines = ssc.textFileStream(args(0));
    lines.print();
    val words = lines.flatMap(_.split(" "));;
    val wordsmapCount = words.map(x => (x,1));
    val wordsCount = wordsmapCount.reduceByKey(_+_);

    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()

  }

}

package com.training.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by Nagendra Amalakanta on 5/21/16.
 */
public class JavaTextFileStreamWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: JavaTextFileStreamWordCount <directoryName> <hdfspath>");
            System.exit(1);
        }

        // Create the context with a 10 seconds batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaTextFileStreamWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaDStream<String> linesDstream = ssc.textFileStream(args[0]);

        linesDstream.print();

        JavaDStream<String> words = linesDstream.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String x) throws Exception {
                return Arrays.asList(SPACE.split(x));

            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.print();

        //wordCounts.saveAsHadoopFiles(args[1] ,"textstream",Text.class, IntWritable.class, TextOutputFormat.class);

        ssc.start();

        ssc.awaitTermination();
    }
}

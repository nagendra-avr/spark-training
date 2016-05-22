package com.training.spark.streaming;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by Nagendra Amalakanta on 5/22/16.
 */
public class JavaSocketTextStreamWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: JavaSocketTextStreamWordCount <hostname> <port>");
            System.exit(1);
        }

        // Create the context with a 10 seconds batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaTextFileStreamWordCount").setMaster("local[3]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaDStream<String> linesDstream = ssc.socketTextStream(args[0],Integer.parseInt(args[1]));

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

        //wordCounts.saveAsHadoopFiles("hdfs://localhost:8020/user/nagi/spark/" ,"textstream",Text.class, IntWritable.class, TextOutputFormat.class);

        ssc.start();

        ssc.awaitTermination();
    }
}

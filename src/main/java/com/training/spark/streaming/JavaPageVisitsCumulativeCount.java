package com.training.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Nagendra Amalakanta on 5/24/16.
 */
public class JavaPageVisitsCumulativeCount {

    private static final Pattern COMMA = Pattern.compile(",");

    public static void main(String[] args) {

        if (args.length < 5) {
            System.err.println("Usage: JavaPageVisitsCumulativeCount <zkQuorum> <group> <topics> <numThreads> <checkpoint>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaTextFileStreamWordCount").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        jssc.checkpoint(args[4]);
        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        //Extract the value
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String,String>, String>() {

            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaDStream<PageVisits> pageVisits = lines.flatMap(new FlatMapFunction<String, PageVisits>() {

            List<PageVisits> pageVisitsList = new ArrayList<PageVisits>();
            @Override
            public Iterable<PageVisits> call(String value) throws Exception {
                String[] visits = value.split(",");
                PageVisits visit = new PageVisits();
                visit.setDate(visits[0]);
                visit.setHostname(visits[1]);
                visit.setIpAddress(visits[2]);
                visit.setHits(Integer.parseInt(visits[3]));
                pageVisitsList.add(visit);

                return pageVisitsList;
            }
        });

        JavaDStream<PageVisits> filteredVisits = pageVisits.filter(new Function<PageVisits, Boolean>() {
            @Override
            public Boolean call(PageVisits pageVisits) throws Exception {
                return pageVisits.getHits() > 5;
            }
        });

        JavaPairDStream<String, Integer> keypageVisits =  pageVisits.mapToPair(new PairFunction<PageVisits, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(PageVisits pageVisits) throws Exception {
                return new Tuple2<String, Integer>(pageVisits.getIpAddress(), pageVisits.getHits());
            }
        });

        JavaPairDStream reducebyKeyvisits =  keypageVisits.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaPairDStream<String, Integer> endpointCountsDStream = reducebyKeyvisits.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> current) throws Exception {

                Integer sum = current.or(0);
                for(Integer value : integers) {
                    sum += value;
                }
                return Optional.of(sum);
            }
        });

        endpointCountsDStream.print();

        jssc.start();

        jssc.awaitTermination();

    }
}

package com.training.spark.streaming;


import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by Nagendra Amalakanta on 5/22/16.
 */
public class KafkaProducer {



    public static void main(String[] args) {

        if(args.length < 2) {
            System.err.println("Usage: KafkaProducer <TopicName> <BrokerList>");
            System.exit(1);
        }
            String topic = args[0];
            String brokerList = args[1];
            new KafkaProducer().sendMessage(topic, brokerList);

    }

    private void sendMessage(String topic, String brokerList) {


        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        Random random = new Random();
        ProducerConfig config = new ProducerConfig(props);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(config);

        while(true) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + random.nextInt(255);
            int hits = random.nextInt(10);
            String msg = runtime + ",www.example.com,"+ ip+","+hits;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
            producer.send(data);
     }
    }
}

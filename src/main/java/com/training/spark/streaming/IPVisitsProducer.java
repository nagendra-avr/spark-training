package com.training.spark.streaming;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

/**
 * Created by Nagendra Amalakanta on 5/24/16.
 */
public class IPVisitsProducer {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: <topicName> <messagesPerSec>");
            System.exit(1);
        }

        final String topic = args[0];
        final String brokerList = args[1];

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                int j = 1;
                while (j <= 100) {

                    Properties props = new Properties();
                    props.put("metadata.broker.list", brokerList);
                    props.put("serializer.class", "kafka.serializer.StringEncoder");
                    props.put("request.required.acks", "1");

                    Random random = new Random();
                    ProducerConfig config = new ProducerConfig(props);
                    kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(config);

                        long runtime = new Date().getTime();
                        String ip = "192.168.2." + random.nextInt(255);
                        int hits = random.nextInt(10);
                        String msg = runtime + ",www.example.com,"+ ip+","+hits;
                        System.out.println("Msg ===>" + msg);
                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
                        producer.send(data);
                        j++;
                }
            }

        };
        Timer timer = new Timer();

        timer.schedule(task,new Date(),	3000);

    }

}

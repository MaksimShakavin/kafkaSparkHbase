package com.epam.bigdata2016.minskq3.kafka;


import com.epam.bigdata2016.minskq3.conf.AppProperties;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaProcessor {
    public static JavaPairReceiverInputDStream<String,String> getStream(JavaStreamingContext jsc, AppProperties.KafkaConnection kafkaConf){
        return KafkaUtils.createStream(
                jsc,
                kafkaConf.getZookeeper(),
                kafkaConf.getGroup(),
                kafkaConf.getTopics()
        );
    }
}

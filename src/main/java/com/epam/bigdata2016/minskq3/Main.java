package com.epam.bigdata2016.minskq3;


import com.epam.bigdata2016.minskq3.conf.AppProperties;
import com.epam.bigdata2016.minskq3.hbase.HbaseProcessor;
import com.epam.bigdata2016.minskq3.kafka.KafkaProcessor;
import com.epam.bigdata2016.minskq3.model.LogLine;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;


@ComponentScan
@EnableAutoConfiguration
public class Main {


    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Main.class).run(args);
        AppProperties props = ctx.getBean(AppProperties.class);
        JavaStreamingContext jsc = ctx.getBean(JavaStreamingContext.class);


        JavaPairReceiverInputDStream<String, String> logs =
                KafkaProcessor.getStream(jsc, props.getKafkaConnection());

        logs.print();
        logs.map(keyValue -> LogLine.parseLogLine(keyValue._2()))
                .foreachRDD(rdd ->
                        rdd.map(line -> LogLine.convertToPut(line, props.getHbase().getColumnFamily()))
                                .foreachPartition(iter -> HbaseProcessor.saveToTable(iter, props.getHbase()))
                );

        jsc.start();
        jsc.awaitTermination();
    }

}

package com.epam.bigdata2016.minskq3;


import com.epam.bigdata2016.minskq3.conf.AppProperties;
import com.epam.bigdata2016.minskq3.hbase.HbaseProcessor;
import com.epam.bigdata2016.minskq3.kafka.KafkaProcessor;
import com.epam.bigdata2016.minskq3.model.CityInfo;
import com.epam.bigdata2016.minskq3.model.ESModel;
import com.epam.bigdata2016.minskq3.model.LogLine;
import com.epam.bigdata2016.minskq3.utils.DictionaryUtils;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.Map;


@ComponentScan
@EnableAutoConfiguration
public class Main {


    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Main.class).run(args);
        AppProperties props = ctx.getBean(AppProperties.class);
        JavaStreamingContext jsc = ctx.getBean(JavaStreamingContext.class);

        Broadcast<Map<String, CityInfo>> brCitiesDict = jsc.sparkContext().broadcast(DictionaryUtils.citiesDictionry());

        JavaPairReceiverInputDStream<String, String> logs =
                KafkaProcessor.getStream(jsc, props.getKafkaConnection());

        //save to HBASE
        JavaDStream<LogLine> logLineStream = logs.map(keyValue -> LogLine.parseLogLine(keyValue._2()));
        logLineStream
                .foreachRDD(rdd ->
                        rdd.map(line -> LogLine.convertToPut(line, props.getHbase().getColumnFamily()))
                                .foreachPartition(iter -> HbaseProcessor.saveToTable(iter, props.getHbase()))
                );
        //save to ELASTIC SEARCH
        logs
                .map(keyValue -> {
                    ESModel model = ESModel.parseLine(keyValue._2());
                    CityInfo cityInfo = brCitiesDict.value().get(Integer.toString(model.getCity()));
                    model.setCityInfo(cityInfo);
                    return ESModel.parseLine(keyValue._2());
                })
                .map(ESModel::toStringifyJson)
                .foreachRDD(jsonRdd -> JavaEsSpark.saveJsonToEs(jsonRdd, args[5] + "/" + args[6]/*"logsindext3/logs"*/));
        jsc.start();
        jsc.awaitTermination();
    }

}

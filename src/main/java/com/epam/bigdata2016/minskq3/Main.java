package com.epam.bigdata2016.minskq3;


import com.epam.bigdata2016.minskq3.conf.AppProperties;
import com.epam.bigdata2016.minskq3.hbase.HbaseProcessor;
import com.epam.bigdata2016.minskq3.kafka.KafkaProcessor;
import com.epam.bigdata2016.minskq3.model.CityInfo;
import com.epam.bigdata2016.minskq3.model.ESModel;
import com.epam.bigdata2016.minskq3.model.LogLine;
import com.epam.bigdata2016.minskq3.utils.DictionaryUtils;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;

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

        //Prepare dictionaries
        DictionaryUtils dictionaryUtils = ctx.getBean(DictionaryUtils.class);
        Map<String, CityInfo> cityDict = dictionaryUtils.citiesDictionry();
        Map<String, String> tagsDict = dictionaryUtils.tagsDictionary();
        Broadcast<Map<String, CityInfo>> brCitiesDict = jsc.sparkContext().broadcast(cityDict);
        Broadcast<Map<String, String>> brTagsDict = jsc.sparkContext().broadcast(tagsDict);


        HbaseProcessor hbaseProcessor = ctx.getBean(HbaseProcessor.class);
        LogisticRegressionModel model1 = LogisticRegressionModel.load(jsc.sparkContext().sc(), props.getSpark().getPathToMlibModel());


        //read from kafka
        JavaPairReceiverInputDStream<String, String> logs =
            KafkaProcessor.getStream(jsc, props.getKafkaConnection());

        //save to HBASE
        JavaDStream<LogLine> logLineStream = logs.map(keyValue -> LogLine.parseLogLine(keyValue._2()))
            .filter(line -> !"null".equals(line.getiPinyouId()));
        logLineStream
            .foreachRDD(rdd ->
                rdd.map(line -> LogLine.convertToPut(line, props.getHbase().getColumnFamily()))
                    .foreachPartition(hbaseProcessor::saveToTable)
            );

        //save to ELASTIC SEARCH
        String index = props.getElasticSearch().getIndex();
        String type = props.getElasticSearch().getType();
        String confStr = index+ "/" +type;
        CityInfo unknown = new CityInfo(0,0);
        logs
            .map(keyValue -> {
                ESModel model = ESModel.parseLine(keyValue._2());
                CityInfo cityInfo = brCitiesDict.value().getOrDefault(Integer.toString(model.getCity()),unknown);
                model.setGeoPoint(cityInfo);
                model.setMlResult(model1.predict(Vectors.dense(model.getOsName().hashCode(),
                    model.getDevice().hashCode(),
                    model.getUaFamily().hashCode(),
                    model.getRegion(),
                    model.getCity(),
                    model.getDomain().hashCode(),
                    model.getAddSlotWidth(),
                    model.getAddSlotHeight(),
                    model.getAddSlotVisability(),
                    model.getAddSlotFormat(),
                    brTagsDict.value().get(model.getUserTags()).split(",")[0].hashCode())));
                return model;
            })
            .filter(line -> !"null".equals(line.getiPinyouId()))
            .mapPartitions(hbaseProcessor::getUserCategory)
            .map(ESModel::toStringifyJson)
            .foreachRDD(jsonRdd -> {
                JavaEsSpark.saveJsonToEs(jsonRdd, confStr);
            });


        jsc.start();
        jsc.awaitTermination();
    }

}

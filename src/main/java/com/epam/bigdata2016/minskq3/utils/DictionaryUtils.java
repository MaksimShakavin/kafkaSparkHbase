package com.epam.bigdata2016.minskq3.utils;


import com.epam.bigdata2016.minskq3.conf.AppProperties;
import com.epam.bigdata2016.minskq3.model.CityInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class DictionaryUtils implements Serializable{

    private AppProperties.Hadoop hadoopConf;

    @Autowired
    public DictionaryUtils(AppProperties props) {
        this.hadoopConf = props.getHadoop();
    }

    public Map<String, CityInfo> citiesDictionry() {
        String fileName = hadoopConf.getCityDictionary();
        return onEachLine(fileName,stream -> stream
            .collect(
                Collectors.toMap(
                    line -> line.split("\\t")[0],  // key -id
                    CityInfo::parseLine)           //value - CityInfo
            ));
    }

    public Map<String,String> tagsDictionary() {
        String fileName = hadoopConf.getTagsDictionary();
        return onEachLine(fileName,stream -> stream.skip(1)
            .collect(
                Collectors.toMap(
                    line -> line.split("\\t")[0],
                    line -> line.split("\\t")[1])
            ));
    }

    public <T> T onEachLine(String fileName, Function<Stream<String>,T> handler){
        BufferedReader br=null;
        try {
            FileSystem fs = FileSystem.get(new URI(hadoopConf.getFileSystem()), new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));
            return handler.apply(br.lines());

        } catch (URISyntaxException | IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            try {
                if(br!=null)br.close();
            }catch (Exception ex){
                throw new RuntimeException(ex);
            }
        }
    }

}

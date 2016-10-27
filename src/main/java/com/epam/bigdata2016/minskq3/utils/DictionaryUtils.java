package com.epam.bigdata2016.minskq3.utils;


import com.epam.bigdata2016.minskq3.model.CityInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DictionaryUtils {

    private static Configuration conf;
    private static FileSystem fileSystem;
    private static final String FILE_DESTINATION = "hdfs://sandbox.hortonworks.com:8020/";

    public static void initFileHelper() throws URISyntaxException, IOException {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
    }


    public static Map<String, CityInfo> citiesDictionry() {
        try (FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
             BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://sandbox.hortonworks.com:8020/"))))) {
           return br.lines()
                    .skip(1)
                    .collect(
                            Collectors.toMap(
                                    line -> line.split("\\t")[1],  // key -id
                                    CityInfo::parseLine)           //value - CityInfo
                    );

        } catch (URISyntaxException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void tagsDictionary() {

    }

}

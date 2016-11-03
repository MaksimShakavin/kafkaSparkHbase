package com.epam.bigdata2016.minskq3.hbase;

import com.epam.bigdata2016.minskq3.conf.AppProperties;
import com.epam.bigdata2016.minskq3.model.ESModel;
import com.epam.bigdata2016.minskq3.model.LogLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;

@Component
public class HbaseProcessor implements Serializable{

    private AppProperties.Hbase hbaseConfig;

    @Autowired
    public HbaseProcessor(AppProperties properties) {
        this.hbaseConfig = properties.getHbase();
    }

    private static DateTimeFormatter dateParser = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");
    private static DateTimeFormatter dp = DateTimeFormatter.ofPattern("yyyyMMdd");


    public void saveToTable(Iterator<Put> iter){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZookeeperClientPort());
        conf.set("hbase.zookeeper.quorum", hbaseConfig.getZookeeperQuorum());
        conf.set("zookeeper.znode.parent", hbaseConfig.getZookeeperParent());

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(hbaseConfig.getTableName()))) {

            List<Put> batch = new ArrayList<>();
            iter.forEachRemaining(batch::add);
            table.put(batch);
        }
        catch (IOException ex){
            throw new RuntimeException(ex);
        }
    }

    public  Iterator<ESModel> getUserCategory(Iterator<ESModel> iter){
        String fullUrl = "jdbc:" + hbaseConfig.getZookeeperQuorum() + ":" + hbaseConfig.getZookeeperClientPort() + ":" + hbaseConfig.getZookeeperParent();
        List<ESModel> models = new ArrayList<>();
        iter.forEachRemaining(models::add);
        ResultSet rset = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            java.sql.Connection con = DriverManager.getConnection(fullUrl);
            for (ESModel line: models){
                LocalDate oldDate = LocalDate.parse(line.getTimestamp(), dateParser);

                PreparedStatement statement = con.prepareStatement("select LAL as LAL, COUNT(*) as CLICKS FROM (SELECT SUBSTR(TIMESTAMP_DATE,0,8) as LAL FROM LOG_TABLEF3 WHERE IPINYOU_ID=?) GROUP BY LAL ORDER BY CLICKS");
                statement.setString(1, line.getiPinyouId());
                statement.executeQuery();
                rset = statement.executeQuery();

                long priorDayClicks = 0;

                while (rset.next()) {
                    LocalDate newDate = LocalDate.parse(rset.getString("LAL"), dp);
                    Integer numberOfClicks = rset.getInt("CLICKS");
                    long diff = DAYS.between(newDate, oldDate);
                    if (diff == 1) priorDayClicks += numberOfClicks;
                }
                String result;

                if (priorDayClicks >= 2) result = "CurrentHighRepeat";
                else if (priorDayClicks == 1) result = "CurrentRepeat";
                else result = "New";
                line.setCategory(result);
            }
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }

        return models.iterator();
    }


}

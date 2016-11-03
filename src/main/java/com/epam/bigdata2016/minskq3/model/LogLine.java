package com.epam.bigdata2016.minskq3.model;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.Serializable;

public class LogLine implements Serializable{
    private String bidId;           private static final byte[] bidIdBytes = Bytes.toBytes("BID_ID");
    private String timestamp;       private static final byte[] timestampBytes = Bytes.toBytes("TIMESTAMP_DATE");
    private String iPinyouId;       private static final byte[] iPinyouIdBytes = Bytes.toBytes("IPINYOU_ID");
    private String userAgent;       private static final byte[] userAgentBytes = Bytes.toBytes("USER_AGENT");
    private String ip;              private static final byte[] ipBytes = Bytes.toBytes("IP");
    private int region;             private static final byte[] regionBytes = Bytes.toBytes("REGION");
    private int city;               private static final byte[] cityBytes = Bytes.toBytes("CITY");
    private int addExchange;        private static final byte[] addExchangeBytes = Bytes.toBytes("AD_EXCHANGE");
    private String domain;          private static final byte[] domainBytes = Bytes.toBytes("DOMAIN");
    private String url;             private static final byte[] urlBytes = Bytes.toBytes("URL");
    private String anonUrl;         private static final byte[] anonUrlBytes = Bytes.toBytes("ANONYMOUS_URL_ID");
    private String addSlot;         private static final byte[] addSlotBytes = Bytes.toBytes("AD_SLOT_ID");
    private int addSlotWidth;       private static final byte[] addSlotWidthBytes = Bytes.toBytes("AD_SLOT_WIDTH");
    private int addSlotHeight;      private static final byte[] addSlotHeightBytes = Bytes.toBytes("AD_SLOT_HEIGHT");
    private int addSlotVisability;  private static final byte[] addSlotVisabilityBytes = Bytes.toBytes("AD_SLOT_VISIBILITY");
    private int addSlotFormat;      private static final byte[] addSlotFormatBytes = Bytes.toBytes("AD_SLOT_FORMAT");
    private int payingPrice;        private static final byte[] payingPriceBytes = Bytes.toBytes("PAYING_PRICE");
    private String creativeId;      private static final byte[] creativeIdBytes = Bytes.toBytes("CREATIVE_ID");
    private int biddingPrice;       private static final byte[] biddingPriceBytes = Bytes.toBytes("BIDDING_PRICE");
    private String addvertiseId;    private static final byte[] addvertiseIdBytes = Bytes.toBytes("ADVERTISER_ID");
    private String userTags;        private static final byte[] userTagsBytes = Bytes.toBytes("USER_TAGS");
    private int streamId;           private static final byte[] streamIdBytes = Bytes.toBytes("STREAM_ID");
    private String tagsList;        private static final byte[] tagsListBytes = Bytes.toBytes("TAGS_LIST");
    private CityInfo geoPoint;      private static final byte[] latBytes = Bytes.toBytes("LAT");
                                    private static final byte[] lonBytes = Bytes.toBytes("LON");



    public static Put convertToPut(LogLine line, String columnFamily){
        byte[] callFamilyBytes = Bytes.toBytes(columnFamily);
        String rowKey = line.iPinyouId + "_" + line.timestamp;
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(callFamilyBytes,bidIdBytes,Bytes.toBytes(line.bidId));
        put.addColumn(callFamilyBytes,timestampBytes,Bytes.toBytes(line.timestamp));
        put.addColumn(callFamilyBytes,iPinyouIdBytes,Bytes.toBytes(line.iPinyouId));
        put.addColumn(callFamilyBytes,userAgentBytes,Bytes.toBytes(line.userAgent));
        put.addColumn(callFamilyBytes,ipBytes,Bytes.toBytes(line.ip));
        put.addColumn(callFamilyBytes,cityBytes,Bytes.toBytes(line.city));
        put.addColumn(callFamilyBytes,regionBytes,Bytes.toBytes(line.region));
        put.addColumn(callFamilyBytes,addExchangeBytes,Bytes.toBytes(line.addExchange));
        put.addColumn(callFamilyBytes,domainBytes,Bytes.toBytes(line.domain));
        put.addColumn(callFamilyBytes,urlBytes,Bytes.toBytes(line.url));
        put.addColumn(callFamilyBytes,anonUrlBytes,Bytes.toBytes(line.anonUrl));
        put.addColumn(callFamilyBytes,addSlotBytes,Bytes.toBytes(line.addSlot));
        put.addColumn(callFamilyBytes,addSlotWidthBytes,Bytes.toBytes(line.addSlotWidth));
        put.addColumn(callFamilyBytes,addSlotHeightBytes,Bytes.toBytes(line.addSlotHeight));
        put.addColumn(callFamilyBytes,addSlotVisabilityBytes,Bytes.toBytes(line.addSlotVisability));
        put.addColumn(callFamilyBytes,addSlotFormatBytes,Bytes.toBytes(line.addSlotFormat));
        put.addColumn(callFamilyBytes,payingPriceBytes,Bytes.toBytes(line.payingPrice));
        put.addColumn(callFamilyBytes,creativeIdBytes,Bytes.toBytes(line.creativeId));
        put.addColumn(callFamilyBytes,biddingPriceBytes,Bytes.toBytes(line.biddingPrice));
        put.addColumn(callFamilyBytes,addvertiseIdBytes,Bytes.toBytes(line.addvertiseId));
        put.addColumn(callFamilyBytes,userTagsBytes,Bytes.toBytes(line.userTags));
        put.addColumn(callFamilyBytes,streamIdBytes,Bytes.toBytes(line.streamId));

        put.addColumn(callFamilyBytes, tagsListBytes, Bytes.toBytes(line.tagsList));
        put.addColumn(callFamilyBytes, latBytes, Bytes.toBytes(String.valueOf((line.geoPoint != null) ? line.geoPoint.getLat() : 0)));
        put.addColumn(callFamilyBytes, lonBytes, Bytes.toBytes(String.valueOf((line.geoPoint != null) ? line.geoPoint.getLon() : 0)));
        return put;
    }

    public static LogLine parseLogLine(String line){
        String[] arr= line.split("\\t");
        LogLine logLine = new LogLine();
        logLine.bidId     = arr[0];
        logLine.timestamp = arr[1];
        logLine.iPinyouId = arr[2];
        logLine.userAgent = arr[3];
        logLine.ip        = arr[4];
        logLine.region = Integer.parseInt(arr[5]);
        logLine.city = Integer.parseInt(arr[6]);
        logLine.addExchange = Integer.parseInt( arr[7]);
        logLine.domain = arr[8];
        logLine.url = arr[9];
        logLine.anonUrl = arr[10];
        logLine.addSlot = arr[11];
        logLine.addSlotWidth = Integer.parseInt(arr[12]);
        logLine.addSlotHeight = Integer.parseInt(arr[13]);
        logLine.addSlotVisability = Integer.parseInt(arr[14]);
        logLine.addSlotFormat = Integer.parseInt(arr[15]);
        logLine.payingPrice = Integer.parseInt(arr[16]);
        logLine.creativeId = arr[17];
        logLine.biddingPrice = Integer.parseInt(arr[18]);
        logLine.addvertiseId = arr[19];
        logLine.userTags = arr[20];
        logLine.streamId = Integer.parseInt(arr[21]);
        return logLine;
    }

    @Override
    public String toString() {
        return "LogLine{" +
                "bidId='" + bidId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", iPinyouId='" + iPinyouId + '\'' +
                ", userAgent='" + userAgent + '\'' +
                ", ip='" + ip + '\'' +
                ", region=" + region +
                ", city=" + city +
                ", addExchange=" + addExchange +
                ", domain='" + domain + '\'' +
                ", url='" + url + '\'' +
                ", anonUrl='" + anonUrl + '\'' +
                ", addSlot='" + addSlot + '\'' +
                ", addSlotWidth=" + addSlotWidth +
                ", addSlotHeight=" + addSlotHeight +
                ", addSlotVisability=" + addSlotVisability +
                ", addSlotFormat=" + addSlotFormat +
                ", payingPrice=" + payingPrice +
                ", creativeId='" + creativeId + '\'' +
                ", biddingPrice=" + biddingPrice +
                ", addvertiseId='" + addvertiseId + '\'' +
                ", userTags='" + userTags + '\'' +
                ", streamId=" + streamId +
                '}';
    }

    // GETTERS AND SETTERS


    public String getBidId() {
        return bidId;
    }

    public void setBidId(String bidId) {
        this.bidId = bidId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(String iPinyouId) {
        this.iPinyouId = iPinyouId;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getRegion() {
        return region;
    }

    public void setRegion(int region) {
        this.region = region;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public int getAddExchange() {
        return addExchange;
    }

    public void setAddExchange(int addExchange) {
        this.addExchange = addExchange;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAnonUrl() {
        return anonUrl;
    }

    public void setAnonUrl(String anonUrl) {
        this.anonUrl = anonUrl;
    }

    public String getAddSlot() {
        return addSlot;
    }

    public void setAddSlot(String addSlot) {
        this.addSlot = addSlot;
    }

    public int getAddSlotWidth() {
        return addSlotWidth;
    }

    public void setAddSlotWidth(int addSlotWidth) {
        this.addSlotWidth = addSlotWidth;
    }

    public int getAddSlotHeight() {
        return addSlotHeight;
    }

    public void setAddSlotHeight(int addSlotHeight) {
        this.addSlotHeight = addSlotHeight;
    }

    public int getAddSlotVisability() {
        return addSlotVisability;
    }

    public void setAddSlotVisability(int addSlotVisability) {
        this.addSlotVisability = addSlotVisability;
    }

    public int getAddSlotFormat() {
        return addSlotFormat;
    }

    public void setAddSlotFormat(int addSlotFormat) {
        this.addSlotFormat = addSlotFormat;
    }

    public int getPayingPrice() {
        return payingPrice;
    }

    public void setPayingPrice(int payingPrice) {
        this.payingPrice = payingPrice;
    }

    public String getCreativeId() {
        return creativeId;
    }

    public void setCreativeId(String creativeId) {
        this.creativeId = creativeId;
    }

    public int getBiddingPrice() {
        return biddingPrice;
    }

    public void setBiddingPrice(int biddingPrice) {
        this.biddingPrice = biddingPrice;
    }

    public String getAddvertiseId() {
        return addvertiseId;
    }

    public void setAddvertiseId(String addvertiseId) {
        this.addvertiseId = addvertiseId;
    }

    public String getUserTags() {
        return userTags;
    }

    public void setUserTags(String userTags) {
        this.userTags = userTags;
    }

    public int getStreamId() {
        return streamId;
    }

    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    public String getTagsList() {
        return tagsList;
    }

    public void setTagsList(String tagsList) {
        this.tagsList = tagsList;
    }

    public CityInfo getGeoPoint() {
        return geoPoint;
    }

    public void setGeoPoint(CityInfo geoPoint) {
        this.geoPoint = geoPoint;
    }
}

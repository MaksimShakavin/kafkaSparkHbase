package com.epam.bigdata2016.minskq3.model;


import java.io.Serializable;

public class CityInfo implements Serializable{
    private float lat;
    private float lon;

    public CityInfo(float lat, float lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public CityInfo() {
    }

    public static CityInfo parseLine(String line){
        CityInfo info = new CityInfo();
        String[] params = line.split("\\t");
        info.lat = Float.parseFloat(params[6]);
        info.lon = Float.parseFloat(params[7]);
        return info;
    }

    //GETTERS AND SETTERS


    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public float getLon() {
        return lon;
    }

    public void setLon(float lon) {
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "CityInfo{" +
            "lat=" + lat +
            ", lon=" + lon +
            '}';
    }
}

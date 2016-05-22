package com.training.spark.streaming;

import java.io.Serializable;

/**
 * Created by Nagendra Amalakanta on 5/22/16.
 */
public class PageVisits implements Serializable{

    private String date;
    private String hostname;
    private String ipAddress;
    private int hits;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getHits() {
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
    }

    @Override
    public String toString() {

        return String.format("[Date = %s, Hostname = %s, IpAddress = %s, Hits = %s", date,hostname,ipAddress,hits);

    }
}

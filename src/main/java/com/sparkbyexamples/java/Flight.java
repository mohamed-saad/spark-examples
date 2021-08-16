package com.sparkbyexamples.java;

import java.io.Serializable;

public class Flight {

    private long crsarrtime;
    private long crsdephour;
    private long crsdeptime;
    private long month;
    private long dofW;
    private double arrdelay;
    private double crselapsedtime;
    private double depdelay;
    private double dist;
    private String dst;
    private String src;
    private String fldate;
    private String id;
    private String carrier;

    public double getArrdelay() {
        return arrdelay;
    }

    public void setArrdelay(double arrdelay) {
        this.arrdelay = arrdelay;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public long getCrsarrtime() {
        return crsarrtime;
    }

    public void setCrsarrtime(long crsarrtime) {
        this.crsarrtime = crsarrtime;
    }

    public long getCrsdephour() {
        return crsdephour;
    }

    public void setCrsdephour(long crsdephour) {
        this.crsdephour = crsdephour;
    }

    public long getCrsdeptime() {
        return crsdeptime;
    }

    public void setCrsdeptime(long crsdeptime) {
        this.crsdeptime = crsdeptime;
    }

    public double getCrselapsedtime() {
        return crselapsedtime;
    }

    public void setCrselapsedtime(double crselapsedtime) {
        this.crselapsedtime = crselapsedtime;
    }

    public double getDepdelay() {
        return depdelay;
    }

    public void setDepdelay(double depdelay) {
        this.depdelay = depdelay;
    }

    public double getDist() {
        return dist;
    }

    public void setDist(double dist) {
        this.dist = dist;
    }

    public long getDofW() {
        return dofW;
    }

    public void setDofW(long dofW) {
        this.dofW = dofW;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public String getFldate() {
        return fldate;
    }

    public void setFldate(String fldate) {
        this.fldate = fldate;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getMonth() {
        return month;
    }

    public void setMonth(long month) {
        this.month = month;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }
}

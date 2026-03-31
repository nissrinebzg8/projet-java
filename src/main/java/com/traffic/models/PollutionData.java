package com.traffic.models;

public class PollutionData {
    private String zone;
    private int pollution;

    public PollutionData() {
    }

    public PollutionData(String zone, int pollution) {
        this.zone = zone;
        this.pollution = pollution;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public int getPollution() {
        return pollution;
    }

    public void setPollution(int pollution) {
        this.pollution = pollution;
    }
}

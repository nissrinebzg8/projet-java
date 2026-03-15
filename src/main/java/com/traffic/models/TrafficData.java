package com.traffic.models;

public class TrafficData {

    private String zone;        
    private int vehicles;       
    private int pollution;      
    private int noise;          
    private boolean accident;   
    private long timestamp;     

    public TrafficData() {
        this.timestamp = System.currentTimeMillis();
    }

    public TrafficData(String zone, int vehicles, int pollution, int noise, boolean accident) {
        this.zone      = zone;
        this.vehicles  = vehicles;
        this.pollution = pollution;
        this.noise     = noise;
        this.accident  = accident;
        this.timestamp = System.currentTimeMillis();
    }

    
    public String toKafkaMessage() {
        return String.format("zone=%s,vehicles=%d,pollution=%d,noise=%d,accident=%b",
                zone, vehicles, pollution, noise, accident);
    }

    
    public static TrafficData fromKafkaMessage(String message) {
        TrafficData data = new TrafficData();
        String[] parts = message.split(",");
        for (String part : parts) {
            String[] kv = part.split("=");
            if (kv.length != 2) continue;
            switch (kv[0].trim()) {
                case "zone"       -> data.zone      = kv[1].trim();
                case "vehicles"   -> data.vehicles  = Integer.parseInt(kv[1].trim());
                case "pollution"  -> data.pollution = Integer.parseInt(kv[1].trim());
                case "noise"      -> data.noise     = Integer.parseInt(kv[1].trim());
                case "accident"   -> data.accident  = Boolean.parseBoolean(kv[1].trim());
            }
        }
        data.timestamp = System.currentTimeMillis();
        return data;
    }

    
    public String  getZone()      { return zone; }
    public int     getVehicles()  { return vehicles; }
    public int     getPollution() { return pollution; }
    public int     getNoise()     { return noise; }
    public boolean isAccident()   { return accident; }
    public long    getTimestamp() { return timestamp; }

    public void setZone(String zone)          { this.zone = zone; }
    public void setVehicles(int vehicles)     { this.vehicles = vehicles; }
    public void setPollution(int pollution)   { this.pollution = pollution; }
    public void setNoise(int noise)           { this.noise = noise; }
    public void setAccident(boolean accident) { this.accident = accident; }
    public void setTimestamp(long timestamp)  { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("[TrafficData] zone=%s | vehicles=%d | pollution=%d | noise=%d | accident=%b",
                zone, vehicles, pollution, noise, accident);
    }
}
package com.traffic.models;

/**
 * Modèle représentant les données d'un capteur de trafic urbain.
 * Format du message Kafka : zone=A1,vehicles=120,pollution=75,noise=60,accident=false
 */
public class TrafficData {

    private String zone;        // Ex: A1, B2, C3
    private int vehicles;       // Nombre de véhicules/min
    private int pollution;      // Niveau de pollution (µg/m³)
    private int noise;          // Niveau de bruit (dB)
    private boolean accident;   // Accident détecté
    private long timestamp;     // Horodatage Unix

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

    /**
     * Sérialise l'objet en message Kafka (format clé=valeur)
     * Exemple : zone=A1,vehicles=120,pollution=75,noise=60,accident=false
     */
    public String toKafkaMessage() {
        return String.format("zone=%s,vehicles=%d,pollution=%d,noise=%d,accident=%b",
                zone, vehicles, pollution, noise, accident);
    }

    /**
     * Désérialise un message Kafka en objet TrafficData
     */
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

    // ─── Getters & Setters ───────────────────────────────────────────────────

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
package com.traffic.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.traffic.models.TrafficData;

public class TrafficDataStore {

    private static final TrafficDataStore INSTANCE = new TrafficDataStore();
    public static TrafficDataStore getInstance() { return INSTANCE; }
    private TrafficDataStore() {}

    private final Map<String, TrafficData> latestByZone = new ConcurrentHashMap<>();

    private final CopyOnWriteArrayList<TrafficData> history = new CopyOnWriteArrayList<>();
    private static final int MAX_HISTORY = 50;

    private final CopyOnWriteArrayList<String> alerts = new CopyOnWriteArrayList<>();
    private static final int MAX_ALERTS = 20;


    private static final int SEUIL_VEHICULES = 100;
    private static final int SEUIL_POLLUTION = 80;
    private static final int SEUIL_BRUIT     = 85;

    
    public void update(TrafficData data) {
        if (data == null || data.getZone() == null) return;

        // Mettre à jour la dernière valeur pour cette zone
        latestByZone.put(data.getZone(), data);

        // Ajouter à l'historique (limité à MAX_HISTORY)
        history.add(data);
        if (history.size() > MAX_HISTORY) {
            history.remove(0);
        }

        
        generateAlerts(data);
    }

    
    public String getLatestJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"zones\":[");

        List<TrafficData> zones = new ArrayList<>(latestByZone.values());
        for (int i = 0; i < zones.size(); i++) {
            sb.append(toJson(zones.get(i)));
            if (i < zones.size() - 1) sb.append(",");
        }

        sb.append("],");
        sb.append("\"totalVehicles\":").append(getTotalVehicles()).append(",");
        sb.append("\"alertCount\":").append(getAlertZonesCount()).append(",");
        sb.append("\"maxPollution\":").append(getMaxPollution()).append(",");
        sb.append("\"maxNoise\":").append(getMaxNoise()).append(",");
        sb.append("\"alerts\":").append(getAlertsJson());
        sb.append("}");

        return sb.toString();
    }

    
    public String getHistoryJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        List<TrafficData> snap = new ArrayList<>(history);
        for (int i = 0; i < snap.size(); i++) {
            sb.append(toJson(snap.get(i)));
            if (i < snap.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    
    private int getTotalVehicles() {
        return latestByZone.values().stream()
                .mapToInt(TrafficData::getVehicles).sum();
    }

    private long getAlertZonesCount() {
        return latestByZone.values().stream()
                .filter(d -> d.getVehicles() > SEUIL_VEHICULES || d.isAccident())
                .count();
    }

    private int getMaxPollution() {
        return latestByZone.values().stream()
                .mapToInt(TrafficData::getPollution).max().orElse(0);
    }

    private int getMaxNoise() {
        return latestByZone.values().stream()
                .mapToInt(TrafficData::getNoise).max().orElse(0);
    }

    private String getAlertsJson() {
        StringBuilder sb = new StringBuilder("[");
        List<String> snap = new ArrayList<>(alerts);
        for (int i = 0; i < snap.size(); i++) {
            sb.append("\"").append(snap.get(i).replace("\"", "'")).append("\"");
            if (i < snap.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    
    private void generateAlerts(TrafficData data) {
        String zone = data.getZone();

        if (data.isAccident()) {
            addAlert("ACCIDENT — " + zone + " → Déviation activée");
        }
        if (data.getVehicles() > SEUIL_VEHICULES) {
            addAlert("CONGESTION — " + zone + " (" + data.getVehicles() + " veh/min) → Allonger feu vert");
        }
        if (data.getPollution() > SEUIL_POLLUTION) {
            addAlert("POLLUTION — " + zone + " (" + data.getPollution() + " µg/m³) → Réduire trafic");
        }
        if (data.getNoise() > SEUIL_BRUIT) {
            addAlert("BRUIT — " + zone + " (" + data.getNoise() + " dB) → Alerte sonore");
        }
    }

    private void addAlert(String alert) {
        alerts.add(0, alert); // plus récent en premier
        if (alerts.size() > MAX_ALERTS) {
            alerts.remove(alerts.size() - 1);
        }
    }

    private String toJson(TrafficData d) {
        return String.format(
            "{\"zone\":\"%s\",\"vehicles\":%d,\"pollution\":%d,\"noise\":%d,\"accident\":%b,\"timestamp\":%d}",
            d.getZone(), d.getVehicles(), d.getPollution(), d.getNoise(),
            d.isAccident(), d.getTimestamp()
        );
    }
}

package com.traffic.api;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.traffic.models.TrafficData;

public class TrafficDataStore {

    private static final TrafficDataStore INSTANCE = new TrafficDataStore();

    public static TrafficDataStore getInstance() {
        return INSTANCE;
    }

    private static final int MAX_HISTORY = 50;
    private static final int MAX_ALERTS = 20;
    private static final int SEUIL_VEHICULES = 100;
    private static final int SEUIL_POLLUTION = 80;
    private static final int SEUIL_BRUIT = 85;

    private final Map<String, TrafficData> latestByZone = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<TrafficData> history = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> alerts = new CopyOnWriteArrayList<>();

    private TrafficDataStore() {
    }

    public void update(TrafficData data) {
        if (data == null || data.getZone() == null || data.getZone().isBlank()) {
            return;
        }

        latestByZone.put(data.getZone(), data);
        history.add(data);
        if (history.size() > MAX_HISTORY) {
            history.remove(0);
        }

        generateAlerts(data);
    }

    public String getLatestJson() {
        List<TrafficData> zones = new ArrayList<>(latestByZone.values());
        zones.sort(Comparator.comparing(TrafficData::getZone));

        StringBuilder sb = new StringBuilder();
        sb.append("{\"zones\":[");
        for (int i = 0; i < zones.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(toJson(zones.get(i)));
        }
        sb.append("],");
        sb.append("\"totalVehicles\":").append(getTotalVehicles()).append(',');
        sb.append("\"alertCount\":").append(getAlertZonesCount()).append(',');
        sb.append("\"maxPollution\":").append(getMaxPollution()).append(',');
        sb.append("\"maxNoise\":").append(getMaxNoise()).append(',');
        sb.append("\"alerts\":").append(getAlertsJson());
        sb.append('}');
        return sb.toString();
    }

    public String getHistoryJson() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        List<TrafficData> snapshot = new ArrayList<>(history);
        for (int i = 0; i < snapshot.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(toJson(snapshot.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private int getTotalVehicles() {
        return latestByZone.values().stream().mapToInt(TrafficData::getVehicles).sum();
    }

    private long getAlertZonesCount() {
        return latestByZone.values().stream()
            .filter(d -> d.getVehicles() > SEUIL_VEHICULES || d.isAccident())
            .count();
    }

    private int getMaxPollution() {
        return latestByZone.values().stream().mapToInt(TrafficData::getPollution).max().orElse(0);
    }

    private int getMaxNoise() {
        return latestByZone.values().stream().mapToInt(TrafficData::getNoise).max().orElse(0);
    }

    private String getAlertsJson() {
        StringBuilder sb = new StringBuilder("[");
        List<String> snapshot = new ArrayList<>(alerts);
        for (int i = 0; i < snapshot.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(escapeJson(snapshot.get(i))).append('"');
        }
        sb.append(']');
        return sb.toString();
    }

    private void generateAlerts(TrafficData data) {
        String zone = data.getZone();

        if (data.isAccident()) {
            addAlert("ACCIDENT - " + zone + " -> Deviation activee");
        }
        if (data.getVehicles() > SEUIL_VEHICULES) {
            addAlert("CONGESTION - " + zone + " (" + data.getVehicles() + " veh/min) -> Allonger feu vert");
        }
        if (data.getPollution() > SEUIL_POLLUTION) {
            addAlert("POLLUTION - " + zone + " (" + data.getPollution() + " ug/m3) -> Reduire trafic");
        }
        if (data.getNoise() > SEUIL_BRUIT) {
            addAlert("BRUIT - " + zone + " (" + data.getNoise() + " dB) -> Alerte sonore");
        }
    }

    private void addAlert(String alert) {
        alerts.add(0, alert);
        if (alerts.size() > MAX_ALERTS) {
            alerts.remove(alerts.size() - 1);
        }
    }

    private String toJson(TrafficData data) {
        return String.format(
            "{\"zone\":\"%s\",\"vehicles\":%d,\"pollution\":%d,\"noise\":%d,\"accident\":%b,\"timestamp\":%d}",
            escapeJson(data.getZone()),
            data.getVehicles(),
            data.getPollution(),
            data.getNoise(),
            data.isAccident(),
            data.getTimestamp()
        );
    }

    private String escapeJson(String value) {
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\r", "\\r")
            .replace("\n", "\\n");
    }
}

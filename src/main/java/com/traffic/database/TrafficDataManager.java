package com.traffic.database;

import java.util.concurrent.ThreadLocalRandom;

import com.traffic.models.FluxVehiculesData;
import com.traffic.models.PollutionData;
import com.traffic.models.TrafficData;
import com.traffic.services.CameraServiceImpl;
import com.traffic.services.ServiceFluxVehicules;

public class TrafficDataManager {

    private static final int VEHICLE_ALERT_THRESHOLD = 100;
    private static final int POLLUTION_ALERT_THRESHOLD = 80;
    private static final int NOISE_ALERT_THRESHOLD = 85;

    private final TrafficRepository repository;

    public TrafficDataManager() {
        this.repository = new TrafficRepository();
    }

    public void save(TrafficData data) {
        if (data == null || data.getZone() == null || data.getZone().isBlank()) {
            System.out.println("[DB] Data ignored: missing zone.");
            return;
        }

        repository.insertTrafic(data.getZone(), data.getVehicles());
        repository.insertPollution(data.getZone(), data.getPollution());
        repository.insertBruit(data.getZone(), data.getNoise());
        repository.insertAccident(data.getZone(), data.isAccident());
        saveAlerts(data);

        System.out.printf("[DB] Stored metrics for zone %s.%n", data.getZone());
    }

    public void collectAndStoreData() {
        try {
            ServiceFluxVehicules serviceFlux = new ServiceFluxVehicules();
            FluxVehiculesData fluxData = serviceFlux.getFluxVehicules("Av_Fal_Ould_Oumeir");

            PollutionData pollutionData = new PollutionData(
                "Av_Oqba_Ibn_Naafi",
                ThreadLocalRandom.current().nextInt(30, 101)
            );

            CameraServiceImpl cameraService = new CameraServiceImpl();
            boolean accident = cameraService.accidentDetecte("Carrefour_Arribat");

            int bruit = ThreadLocalRandom.current().nextInt(40, 101);

            save(new TrafficData(
                fluxData.getRoute(),
                fluxData.getNombreVehicules(),
                pollutionData.getPollution(),
                bruit,
                accident
            ));

            System.out.println("Collect and storage completed.");
        } catch (Exception e) {
            System.out.println("TrafficDataManager error: " + e.getMessage());
        }
    }

    public void close() {
        // Nothing to close. Method kept for compatibility with existing demo code.
    }

    private void saveAlerts(TrafficData data) {
        if (data.getVehicles() > VEHICLE_ALERT_THRESHOLD) {
            repository.insertAlerte(
                "CONGESTION",
                String.format("%s exceeds vehicle threshold (%d veh/min)", data.getZone(), data.getVehicles())
            );
        }
        if (data.getPollution() > POLLUTION_ALERT_THRESHOLD) {
            repository.insertAlerte(
                "POLLUTION",
                String.format("%s exceeds pollution threshold (%d ug/m3)", data.getZone(), data.getPollution())
            );
        }
        if (data.getNoise() > NOISE_ALERT_THRESHOLD) {
            repository.insertAlerte(
                "BRUIT",
                String.format("%s exceeds noise threshold (%d dB)", data.getZone(), data.getNoise())
            );
        }
        if (data.isAccident()) {
            repository.insertAlerte(
                "ACCIDENT",
                String.format("Accident detected in %s", data.getZone())
            );
        }
    }
}

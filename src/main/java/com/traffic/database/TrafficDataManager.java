package com.traffic.database;

import com.traffic.models.FluxVehiculesData;
import com.traffic.models.PollutionData;
import com.traffic.services.CameraServiceImpl;
import com.traffic.services.ServiceFluxVehicules;
import java.util.concurrent.ThreadLocalRandom;

public class TrafficDataManager {

    public void collectAndStoreData() {
        try {
            TrafficRepository repository = new TrafficRepository();

            // 1) Flux véhicules
            ServiceFluxVehicules serviceFlux = new ServiceFluxVehicules();
            FluxVehiculesData fluxData = serviceFlux.getFluxVehicules("RouteA");
            repository.insertTrafic(fluxData.getRoute(), fluxData.getNombreVehicules());

            // 2) Pollution simulée
            PollutionData pollutionData = new PollutionData(
                "ZoneA",
                ThreadLocalRandom.current().nextInt(30, 101)
            );
            repository.insertPollution(pollutionData.getZone(), pollutionData.getPollution());

            // 3) Accident
            CameraServiceImpl cameraService = new CameraServiceImpl();
            boolean accident = cameraService.accidentDetecte("CarrefourA");
            repository.insertAccident("CarrefourA", accident);

            // 4) Bruit simulé
            int bruit = ThreadLocalRandom.current().nextInt(40, 101);
            repository.insertBruit("ZoneA", bruit);

            System.out.println("Collecte et stockage terminés.");
        } catch (Exception e) {
            System.out.println("Erreur dans TrafficDataManager : " + e.getMessage());
        }
    }
}
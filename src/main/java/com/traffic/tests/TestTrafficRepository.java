package com.traffic.tests;

import com.traffic.database.TrafficRepository;

public class TestTrafficRepository {
    public static void main(String[] args) {
        // Creation du repository
        TrafficRepository repository = new TrafficRepository();

        // Insertion des donnees de test
        repository.insertTrafic("RouteA", 120);
        repository.insertPollution("ZoneA", 78);
        repository.insertAccident("CarrefourA", true);
        repository.insertBruit("ZoneA", 85);
        repository.insertAlerte("Congestion", "Embouteillage détecté sur RouteA");

        // Message final
        System.out.println("Insertions effectuées avec succès.");
    }
}

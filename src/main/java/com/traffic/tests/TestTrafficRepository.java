package com.traffic.tests;

import com.traffic.database.TrafficRepository;

public class TestTrafficRepository {
    public static void main(String[] args) {
        // Creation du repository
        TrafficRepository repository = new TrafficRepository();

        // Insertion des donnees de test
        repository.insertTrafic("Av_Fal_Ould_Oumeir", 120);
        repository.insertPollution("Av_Oqba_Ibn_Naafi", 78);
        repository.insertAccident("Carrefour_Arribat", true);
        repository.insertBruit("Av_Oqba_Ibn_Naafi", 85);
        repository.insertAlerte("Congestion", "Embouteillage détecté sur RouteA");

        // Message final
        System.out.println("Insertions effectuées avec succès.");
    }
}

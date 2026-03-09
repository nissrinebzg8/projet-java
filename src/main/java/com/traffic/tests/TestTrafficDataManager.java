package com.traffic.tests;

import com.traffic.database.TrafficDataManager;

public class TestTrafficDataManager {
    public static void main(String[] args) {
        // Creation du gestionnaire central des donnees trafic
        TrafficDataManager manager = new TrafficDataManager();

        // Lance la collecte des donnees puis leur stockage en base
        manager.collectAndStoreData();

        // Message final de confirmation
        System.out.println("Collecte et stockage terminés");
    }
}

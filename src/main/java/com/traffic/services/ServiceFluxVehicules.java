package com.traffic.services;

import java.util.concurrent.ThreadLocalRandom;

import com.traffic.models.FluxVehiculesData;

public class ServiceFluxVehicules {

    // Simule le flux de vehicules pour une route donnee
    public FluxVehiculesData getFluxVehicules(String route) {
        // Si la route est vide ou nulle, on prend une valeur par defaut
        if (route == null || route.trim().isEmpty()) {
            route = "Av_Fal_Ould_Oumeir";
        }

        // Nombre aleatoire entre 0 et 150 inclus
        int nombreVehicules = ThreadLocalRandom.current().nextInt(0, 151);
        return new FluxVehiculesData(route, nombreVehicules);
    }
}

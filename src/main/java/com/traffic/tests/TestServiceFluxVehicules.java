package com.traffic.tests;

import com.traffic.models.FluxVehiculesData;
import com.traffic.services.ServiceFluxVehicules;

public class TestServiceFluxVehicules {
    public static void main(String[] args) {
        // Creation du service
        ServiceFluxVehicules service = new ServiceFluxVehicules();

        // Appel de la methode avec la route "RouteA"
        FluxVehiculesData resultat = service.getFluxVehicules("RouteA");

        // Affichage du resultat dans la console
        System.out.println(resultat);
    }
}

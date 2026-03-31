package com.traffic.tests;

import com.traffic.models.FluxVehiculesData;
import com.traffic.services.ServiceFluxVehicules;

public class TestServiceFluxVehicules {
    public static void main(String[] args) {
        // Creation du service
        ServiceFluxVehicules service = new ServiceFluxVehicules();

        // Appel de la methode avec la route "Av_Fal_Ould_Oumeir"
        FluxVehiculesData resultat = service.getFluxVehicules("Av_Fal_Ould_Oumeir");

        // Affichage du resultat dans la console
        System.out.println(resultat);
    }
}

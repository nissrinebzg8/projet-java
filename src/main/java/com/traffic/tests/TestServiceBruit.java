package com.traffic.tests;

import com.traffic.services.ServiceBruitClient;

public class TestServiceBruit {
    public static void main(String[] args) {
        // Creation du client TCP
        ServiceBruitClient client = new ServiceBruitClient();

        // Envoi de la zone "Av_Oqba_Ibn_Naafi" au serveur
        String reponse = client.envoyerZone("Av_Oqba_Ibn_Naafi");

        // Affichage de la reponse recue
        System.out.println("Reponse serveur: " + reponse);
    }
}

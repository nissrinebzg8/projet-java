package com.traffic.tests;

import com.traffic.services.ServiceBruitClient;

public class TestServiceBruit {
    public static void main(String[] args) {
        // Creation du client TCP
        ServiceBruitClient client = new ServiceBruitClient();

        // Envoi de la zone "ZoneA" au serveur
        String reponse = client.envoyerZone("ZoneA");

        // Affichage de la reponse recue
        System.out.println("Reponse serveur: " + reponse);
    }
}

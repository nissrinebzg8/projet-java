package com.traffic.services;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ServiceBruitClient {

    private static final String HOST = "localhost";
    private static final int PORT = 5000;

    // Methode utilitaire pour envoyer une zone et recuperer la reponse
    public String envoyerZone(String zone) {
        try (Socket socket = new Socket(HOST, PORT);
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Envoi de la zone au serveur
            writer.println(zone);

            // Lecture de la reponse du serveur
            return reader.readLine();
        } catch (Exception e) {
            return "Erreur client TCP: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        // Exemple simple d'utilisation du client
        ServiceBruitClient client = new ServiceBruitClient();
        String reponse = client.envoyerZone("ZoneA");
        System.out.println(reponse);
    }
}

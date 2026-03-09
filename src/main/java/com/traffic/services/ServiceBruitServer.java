package com.traffic.services;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;

public class ServiceBruitServer {

    public static final int PORT = 5000;

    public static void main(String[] args) {
        // Le serveur ecoute en continu sur le port 5000
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("ServiceBruitServer demarre sur le port " + PORT);

            while (true) {
                // Attente d'un client
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connecte: " + clientSocket.getInetAddress());

                // Traitement du client dans une methode separee
                traiterClient(clientSocket);
            }
        } catch (Exception e) {
            System.out.println("Erreur serveur: " + e.getMessage());
        }
    }

    private static void traiterClient(Socket clientSocket) {
        try (Socket socket = clientSocket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            // Le client envoie une zone (ex: ZoneA)
            String zone = reader.readLine();
            if (zone == null || zone.trim().isEmpty()) {
                zone = "ZoneA";
            }

            // Simulation du niveau de bruit entre 40 et 100 dB
            int bruitDb = ThreadLocalRandom.current().nextInt(40, 101);

            // Reponse au client au format simple
            writer.println("Zone: " + zone + ", Bruit: " + bruitDb + " dB");
        } catch (Exception e) {
            System.out.println("Erreur client: " + e.getMessage());
        }
    }
}

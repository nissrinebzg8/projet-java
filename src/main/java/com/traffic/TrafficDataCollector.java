package com.traffic;

import com.traffic.kafka.KafkaProducerService;
import com.traffic.models.FluxVehiculesData;
import com.traffic.models.PollutionData;
import com.traffic.models.TrafficData;
import com.traffic.services.CameraServiceImpl;
import com.traffic.services.ServiceBruitClient;
import com.traffic.services.ServiceFluxVehicules;
import com.traffic.services.ServicePollution;

/**
 * TrafficDataCollector
 * ─────────────────────────────────────────────────────────────────
 * Orchestre la collecte depuis tous les services distribués
 * et envoie les données vers Kafka topic "traffic-data".
 *
 * Services connectés :
 *   ✅ ServiceFluxVehicules  → JAX-WS  (simulation)
 *   ✅ ServicePollution      → JAX-RS  (simulation)
 *   ✅ CameraServiceImpl     → RMI
 *   ✅ ServiceBruitClient    → Socket TCP (localhost:5000)
 */
public class TrafficDataCollector {

    // ─── Zones à surveiller ──────────────────────────────────────────────────
    private static final String[] ZONES  = {"RouteA", "RouteB", "CarrefourA", "ZoneA"};

    // ─── Seuils d'alerte ─────────────────────────────────────────────────────
    private static final int SEUIL_VEHICULES = 100;
    private static final int SEUIL_POLLUTION = 80;
    private static final int SEUIL_BRUIT     = 85;

    // ─── Services distribués ─────────────────────────────────────────────────
    private final ServiceFluxVehicules serviceFlux;
    private final ServicePollution     servicePollution;
    private final ServiceBruitClient   serviceBruit;
    private CameraServiceImpl          serviceCamera;

    // ─── Kafka Producer ──────────────────────────────────────────────────────
    private final KafkaProducerService kafkaProducer;

    // ─── Constructeur ────────────────────────────────────────────────────────
    public TrafficDataCollector(KafkaProducerService kafkaProducer) {
        this.kafkaProducer    = kafkaProducer;
        this.serviceFlux      = new ServiceFluxVehicules();
        this.servicePollution = new ServicePollution();
        this.serviceBruit     = new ServiceBruitClient();

        // RMI peut lever une RemoteException au constructeur
        try {
            this.serviceCamera = new CameraServiceImpl();
        } catch (Exception e) {
            System.err.println("[COLLECTOR] ⚠️  CameraService RMI indisponible : " + e.getMessage());
        }
    }

    // ─── Collecte principale ─────────────────────────────────────────────────

    /**
     * Collecte les données de tous les services et les envoie vers Kafka.
     * À appeler périodiquement (ex: toutes les 30 secondes).
     */
    public void collectAll() {
        System.out.println("\n[COLLECTOR] ▶ Début de la collecte sur toutes les zones...");

        for (String zone : ZONES) {
            collectZone(zone);
        }

        System.out.println("[COLLECTOR] ✅ Collecte terminée.\n");
    }

    /**
     * Collecte et envoie les données pour une zone spécifique.
     */
    public void collectZone(String zone) {
        System.out.printf("%n[COLLECTOR] 📍 Zone : %s%n", zone);

        int     vehicles  = collectFluxVehicules(zone);
        int     pollution = collectPollution(zone);
        int     noise     = collectBruit(zone);
        boolean accident  = collectAccident(zone);

        // Construire un message Kafka unifié pour cette zone
        TrafficData data = new TrafficData(zone, vehicles, pollution, noise, accident);
        kafkaProducer.send(data);

        // Afficher les alertes
        afficherAlertes(zone, vehicles, pollution, noise, accident);
    }

    // ─── Collecteurs par service ─────────────────────────────────────────────

    /**
     * 1) JAX-WS — Flux de véhicules
     */
    private int collectFluxVehicules(String zone) {
        try {
            FluxVehiculesData data = serviceFlux.getFluxVehicules(zone);
            int vehicles = data.getNombreVehicules();
            System.out.printf("[JAX-WS]  FluxVehicules → %s : %d veh/min%n", zone, vehicles);
            return vehicles;
        } catch (Exception e) {
            System.err.println("[JAX-WS]  ❌ Erreur FluxVehicules : " + e.getMessage());
            return 0;
        }
    }

    /**
     * 2) JAX-RS — Niveau de pollution
     */
    private int collectPollution(String zone) {
        try {
            PollutionData data = servicePollution.getPollution(zone);
            int pollution = data.getPollution();
            System.out.printf("[JAX-RS]  Pollution     → %s : %d µg/m³%n", zone, pollution);
            return pollution;
        } catch (Exception e) {
            System.err.println("[JAX-RS]  ❌ Erreur Pollution : " + e.getMessage());
            return 0;
        }
    }

    /**
     * 3) Socket TCP — Niveau de bruit (ServiceBruitClient → ServiceBruitServer:5000)
     */
    private int collectBruit(String zone) {
        try {
            String reponse = serviceBruit.envoyerZone(zone);

            // Si le serveur TCP n'est pas démarré, on simule une valeur
            if (reponse == null || reponse.startsWith("Erreur")) {
                System.out.printf("[TCP]     Bruit (simulé) → %s : serveur indisponible%n", zone);
                return (int)(Math.random() * 60) + 40; // valeur simulée entre 40 et 100
            }

            // Le serveur renvoie "noise=75" ou juste "75"
            int noise = parseNoise(reponse);
            System.out.printf("[TCP]     Bruit          → %s : %d dB%n", zone, noise);
            return noise;

        } catch (Exception e) {
            System.err.println("[TCP]     ❌ Erreur Bruit : " + e.getMessage());
            return 0;
        }
    }

    /**
     * 4) RMI — Détection d'accident (CameraServiceImpl)
     */
    private boolean collectAccident(String zone) {
        if (serviceCamera == null) {
            System.out.printf("[RMI]     Camera (indisponible) → %s%n", zone);
            return false;
        }
        try {
            boolean accident = serviceCamera.accidentDetecte(zone);
            System.out.printf("[RMI]     Camera         → %s : accident=%b%n", zone, accident);
            return accident;
        } catch (Exception e) {
            System.err.println("[RMI]     ❌ Erreur Camera : " + e.getMessage());
            return false;
        }
    }

    // ─── Analyse et alertes ──────────────────────────────────────────────────

    private void afficherAlertes(String zone, int vehicles, int pollution, int noise, boolean accident) {
        boolean alerte = false;

        if (vehicles > SEUIL_VEHICULES) {
            System.out.printf("[ALERTE] 🚦 CONGESTION %s (%d veh/min) → Allonger le feu vert%n",
                    zone, vehicles);
            alerte = true;
        }
        if (pollution > SEUIL_POLLUTION) {
            System.out.printf("[ALERTE] 🌫️  POLLUTION  %s (%d µg/m³)  → Réduire le trafic%n",
                    zone, pollution);
            alerte = true;
        }
        if (noise > SEUIL_BRUIT) {
            System.out.printf("[ALERTE] 🔊 BRUIT      %s (%d dB)     → Alerte sonore%n",
                    zone, noise);
            alerte = true;
        }
        if (accident) {
            System.out.printf("[ALERTE] 🚨 ACCIDENT   %s             → Activer déviation%n", zone);
            alerte = true;
        }
        if (!alerte) {
            System.out.printf("[OK]     ✅ %s — Situation normale%n", zone);
        }
    }

    // ─── Utilitaires ─────────────────────────────────────────────────────────

    /**
     * Parse la réponse du serveur TCP.
     * Supporte "75", "noise=75", "bruit=75", "ZoneA:75"
     */
    private int parseNoise(String reponse) {
        try {
        // Format reçu : "Zone: ZoneA, Bruit: 75 dB"
        // On extrait le nombre avant " dB"
            reponse = reponse.trim();
            int idx = reponse.lastIndexOf("Bruit: ");
            if (idx != -1) {
                String partie = reponse.substring(idx + 7); // après "Bruit: "
                partie = partie.replace(" dB", "").trim();
                return Integer.parseInt(partie);
            }
            return 50; // valeur par défaut
        } catch (NumberFormatException e) {
            return 50;
        }
    }
}
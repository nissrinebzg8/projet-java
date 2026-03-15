package com.traffic;

import com.traffic.api.TrafficHttpServer;
import com.traffic.kafka.KafkaConsumerService;
import com.traffic.kafka.KafkaProducerService;
import com.traffic.models.TrafficData;
import com.traffic.services.ServiceBruitServer;

/**
 * Main.java — version finale complète
 * ─────────────────────────────────────────────────────────────────
 * Modes :
 *   producer     → simule l'envoi de données
 *   consumer     → lit Kafka + alimente le DataStore
 *   collector    → collecte depuis les vrais services distribués
 *   server       → Consumer + Serveur HTTP (dashboard)       ✅ NOUVEAU
 *   bruitserver  → Lance le serveur TCP bruit (port 5000)
 *   both         → producer + consumer
 *
 * Usage recommandé pour la démo complète :
 *   Terminal 1 : java -jar ... bruitserver
 *   Terminal 2 : java -jar ... server       (consumer + HTTP)
 *   Terminal 3 : java -jar ... collector    (envoi données réelles)
 */
public class Main {

    public static void main(String[] args) throws Exception {

        String mode = (args.length > 0) ? args[0].toLowerCase() : "both";

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║  Système Distribué - Gestion Trafic Urbain   ║");
        System.out.println("║  Mode : " + mode + "                         ║");
        System.out.println("╚══════════════════════════════════════════════╝\n");

        switch (mode) {
            case "producer"    -> runProducer();
            case "consumer"    -> runConsumer();
            case "collector"   -> runCollector();
            case "server"      -> runServer();        // ✅ NOUVEAU
            case "bruitserver" -> runBruitServer();
            default            -> runBoth();
        }
    }

    // ─── MODE SERVER : Consumer Kafka + Serveur HTTP pour le dashboard ────────
    /**
     * Lance simultanément :
     *   - Le Consumer Kafka (lit les messages et alimente le DataStore)
     *   - Le serveur HTTP sur port 8080 (expose les données en JSON)
     *
     * Le dashboard React fait des requêtes GET /api/traffic toutes les 5s.
     */
    private static void runServer() throws Exception {
        System.out.println("▶ Démarrage du Consumer + Serveur HTTP...\n");

        // 1. Démarrer le serveur HTTP
        TrafficHttpServer httpServer = new TrafficHttpServer();
        httpServer.start();

        // 2. Démarrer le Consumer Kafka dans un thread séparé
        KafkaConsumerService consumer = new KafkaConsumerService();
        Thread consumerThread = new Thread(consumer::start, "KafkaConsumer");
        consumerThread.setDaemon(false);
        consumerThread.start();

        // Arrêt propre avec Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[MAIN] Arrêt...");
            consumer.stop();
            httpServer.stop();
        }));

        System.out.println("\n[SERVER] Tout est prêt !");
        System.out.println("[SERVER] Dashboard API : http://localhost:8080/api/traffic");
        System.out.println("[SERVER] Ouvrez dashboard.html dans votre navigateur.");
        System.out.println("[SERVER] Ctrl+C pour arrêter.\n");
    }

    // ─── MODE PRODUCER ────────────────────────────────────────────────────────
    private static void runProducer() throws InterruptedException {
        System.out.println("▶ Démarrage du Producer (simulation)...\n");
        KafkaProducerService producer = new KafkaProducerService();

        TrafficData[] data = {
            new TrafficData("Av_Fal_Ould_Oumeir",    120, 75,  60, false),
            new TrafficData("Av_Ibn_Sina",     45, 90,  55, false),
            new TrafficData("Carrefour_Arribat", 30, 20,  40, false),
            new TrafficData("Av_Oqba_Ibn_Naafi",      80, 60,  95, true),
            new TrafficData("Av_Fal_Ould_Oumeir",    150, 85,  70, false),
        };

        for (TrafficData d : data) {
            producer.send(d);
            Thread.sleep(1000);
        }
        producer.close();
        System.out.println("\n✅ Messages envoyés.");
    }

    // ─── MODE CONSUMER ───────────────────────────────────────────────────────
    private static void runConsumer() {
        System.out.println("▶ Démarrage du Consumer...\n");
        KafkaConsumerService consumer = new KafkaConsumerService();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop));
        consumer.start();
    }

    // ─── MODE COLLECTOR ──────────────────────────────────────────────────────
    private static void runCollector() throws InterruptedException {
        System.out.println("▶ Démarrage du Collector (services réels)...\n");
        KafkaProducerService producer = new KafkaProducerService();
        TrafficDataCollector collector = new TrafficDataCollector(producer);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        while (true) {
            collector.collectAll();
            System.out.println("[COLLECTOR] Prochaine collecte dans 30 secondes...");
            Thread.sleep(30_000);
        }
    }

    // ─── MODE BRUITSERVER ────────────────────────────────────────────────────
    private static void runBruitServer() {
        System.out.println("▶ Démarrage ServiceBruitServer (TCP port 5000)...");
        ServiceBruitServer.main(new String[]{});
    }

    // ─── MODE BOTH ───────────────────────────────────────────────────────────
    private static void runBoth() throws InterruptedException {
        KafkaConsumerService consumer = new KafkaConsumerService();
        Thread t = new Thread(consumer::start);
        t.setDaemon(true);
        t.start();
        Thread.sleep(2000);
        runProducer();
        t.join();
    }
}

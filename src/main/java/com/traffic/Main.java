package com.traffic;

import com.traffic.kafka.KafkaConsumerService;
import com.traffic.kafka.KafkaProducerService;
import com.traffic.models.TrafficData;
import com.traffic.services.ServiceBruitServer;
/**
 * Main.java — version finale
 * ─────────────────────────────────────────────────────────────────
 * Modes disponibles :
 *   producer   → simule l'envoi de données vers Kafka
 *   consumer   → lit et analyse les données depuis Kafka
 *   collector  → collecte depuis les vrais services distribués → Kafka  ✅ NOUVEAU
 *   both       → producer + consumer simultanément
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {

        String mode = (args.length > 0) ? args[0].toLowerCase() : "both";

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║  Système Distribué - Gestion Trafic Urbain   ║");
        System.out.println("║  Mode : " + mode + "                         ║");
        System.out.println("╚══════════════════════════════════════════════╝\n");

        switch (mode) {
            case "producer"  -> runProducer();
            case "consumer"  -> runConsumer();
            case "collector" -> runCollector();   // ✅ NOUVEAU
            default          -> runBoth();
            case "bruitserver" -> runBruitServer();
        }
    }

    private static void runBruitServer() {
        System.out.println("▶ Démarrage du ServiceBruitServer (TCP port 5000)...");
        ServiceBruitServer.main(new String[]{});        
    }
    // ─── MODE PRODUCER (simulation) ──────────────────────────────────────────
    private static void runProducer() throws InterruptedException {
        System.out.println("▶ Démarrage du Producer (simulation)...\n");
        KafkaProducerService producer = new KafkaProducerService();

        TrafficData[] capteurData = {
            new TrafficData("A1", 120, 75, 60, false),   // CONGESTION
            new TrafficData("B2", 45,  90, 55, false),   // POLLUTION
            new TrafficData("C3", 30,  20, 40, false),   // NORMALE
            new TrafficData("D4", 80,  60, 95, true),    // ACCIDENT + BRUIT
            new TrafficData("A1", 150, 85, 70, false),   // CONGESTION + POLLUTION
        };

        System.out.printf("📤 Envoi de %d messages vers le topic 'traffic-data'...%n%n",
                capteurData.length);

        for (TrafficData data : capteurData) {
            producer.send(data);
            Thread.sleep(1000);
        }

        producer.close();
        System.out.println("\n✅ Tous les messages ont été envoyés.");
    }

    // ─── MODE CONSUMER ───────────────────────────────────────────────────────
    private static void runConsumer() {
        System.out.println("▶ Démarrage du Consumer...\n");
        KafkaConsumerService consumer = new KafkaConsumerService();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[MAIN] Arrêt du consumer...");
            consumer.stop();
        }));

        consumer.start();
    }

    // ─── MODE COLLECTOR — vrais services distribués ──────────────────────────
    /**
     * ✅ NOUVEAU MODE
     * Collecte depuis JAX-WS, JAX-RS, RMI, Socket TCP
     * et envoie vers Kafka en boucle toutes les 30 secondes.
     *
     * Lancement : java -jar target\traffic-application-full.jar collector
     */
    private static void runCollector() throws InterruptedException {
        System.out.println("▶ Démarrage du Collector (services réels)...\n");

        KafkaProducerService producer = new KafkaProducerService();
        TrafficDataCollector collector = new TrafficDataCollector(producer);

        // Arrêt propre avec Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[MAIN] Arrêt du collector...");
            producer.close();
        }));

        // Boucle de collecte toutes les 30 secondes
        int intervalle = 30_000; // ms
        System.out.printf("[COLLECTOR] Collecte toutes les %d secondes. (Ctrl+C pour arrêter)%n%n",
                intervalle / 1000);

        while (true) {
            collector.collectAll();
            System.out.printf("[COLLECTOR] ⏳ Prochaine collecte dans %d secondes...%n",
                    intervalle / 1000);
            Thread.sleep(intervalle);
        }
    }

    // ─── MODE BOTH ───────────────────────────────────────────────────────────
    private static void runBoth() throws InterruptedException {
        System.out.println("▶ Démarrage combiné (Producer + Consumer)\n");

        KafkaConsumerService consumer = new KafkaConsumerService();
        Thread consumerThread = new Thread(consumer::start, "ConsumerThread");
        consumerThread.setDaemon(true);
        consumerThread.start();

        Thread.sleep(2000);
        runProducer();

        System.out.println("\n[MAIN] Consumer en cours... (Ctrl+C pour arrêter)");
        consumerThread.join();
    }
}
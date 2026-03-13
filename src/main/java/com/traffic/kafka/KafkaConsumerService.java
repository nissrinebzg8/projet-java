package com.traffic.kafka;

import com.traffic.database.TrafficDataManager;
import com.traffic.models.TrafficData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * KafkaConsumerService
 * ─────────────────────────────────────────────────────────────────
 * Lit en continu les messages du topic "traffic-data", désérialise
 * chaque message en TrafficData, puis l'analyse et le stocke en BD.
 *
 * Utilisation :
 *   KafkaConsumerService consumer = new KafkaConsumerService();
 *   consumer.start();   // boucle bloquante – lancer dans un Thread
 */
public class KafkaConsumerService {

    // ─── Constantes ──────────────────────────────────────────────────────────
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME        = "traffic-data";
    private static final String GROUP_ID          = "traffic-group";   // groupe de consommateurs

    // Seuils d'alerte
    private static final int SEUIL_VEHICLES  = 100;  // véhicules/min → congestion
    private static final int SEUIL_POLLUTION = 80;   // µg/m³         → alerte environnementale
    private static final int SEUIL_NOISE     = 85;   // dB            → alerte bruit

    // ─── Attributs ───────────────────────────────────────────────────────────
    private final KafkaConsumer<String, String> consumer;
    private final TrafficDataManager            dbManager;
    private volatile boolean                    running = true;

    // ─── Constructeur ────────────────────────────────────────────────────────
    public KafkaConsumerService() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Lire depuis le début si aucun offset enregistré
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");

        // Valider l'offset automatiquement toutes les 5 secondes
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,         "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,    "5000");

        this.consumer  = new KafkaConsumer<>(props);
        this.dbManager = new TrafficDataManager();

        // S'abonner au topic
        consumer.subscribe(List.of(TOPIC_NAME));
        System.out.println("[CONSUMER] Abonné au topic : " + TOPIC_NAME);
    }

    // ─── Boucle principale ───────────────────────────────────────────────────

    /**
     * Démarre la boucle de consommation (bloquante).
     * Appelez cette méthode dans un Thread séparé.
     */
    public void start() {
        System.out.println("[CONSUMER] ▶ En attente de messages...\n");

        try {
            while (running) {
                // Poll : attendre jusqu'à 3 secondes des messages
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(3));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } finally {
            consumer.close();
            System.out.println("[CONSUMER] Connexion fermée.");
        }
    }

    /**
     * Traite un enregistrement Kafka individuel.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        System.out.printf("%n[CONSUMER] 📨 Message reçu → offset=%d | clé=%s%n",
                record.offset(), record.key());
        System.out.printf("[CONSUMER]    Valeur : %s%n", record.value());

        // 1. Désérialiser le message
        TrafficData data = TrafficData.fromKafkaMessage(record.value());
        System.out.println("[CONSUMER]    " + data);

        // 2. Analyser les seuils et générer des recommandations
        analyzeAndRecommend(data);

        // 3. Stocker en base de données MySQL
        dbManager.save(data);
    }

    /**
     * Analyse les données reçues et génère des recommandations automatiques.
     */
    private void analyzeAndRecommend(TrafficData data) {
        System.out.println("[CONSUMER] 🔍 Analyse :");

        if (data.getVehicles() > SEUIL_VEHICLES) {
            System.out.printf("[CONSUMER] ⚠️  CONGESTION zone %s (%d veh/min) → Allonger le feu vert%n",
                    data.getZone(), data.getVehicles());
        }

        if (data.getPollution() > SEUIL_POLLUTION) {
            System.out.printf("[CONSUMER] 🌫️  POLLUTION zone %s (%d µg/m³) → Réduire le trafic%n",
                    data.getZone(), data.getPollution());
        }

        if (data.getNoise() > SEUIL_NOISE) {
            System.out.printf("[CONSUMER] 🔊 BRUIT zone %s (%d dB) → Alerte sonore%n",
                    data.getZone(), data.getNoise());
        }

        if (data.isAccident()) {
            System.out.printf("[CONSUMER] 🚨 ACCIDENT zone %s → Activer déviation de trafic%n",
                    data.getZone());
        }

        if (data.getVehicles() <= SEUIL_VEHICLES
                && data.getPollution() <= SEUIL_POLLUTION
                && !data.isAccident()) {
            System.out.println("[CONSUMER] ✅ Situation normale — aucune action requise.");
        }
    }

    /**
     * Arrête proprement le consumer.
     */
    public void stop() {
        running = false;
    }
}
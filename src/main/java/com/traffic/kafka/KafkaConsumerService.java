package com.traffic.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.traffic.api.TrafficDataStore;
import com.traffic.database.TrafficDataManager;
import com.traffic.models.TrafficData;

public class KafkaConsumerService {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME        = "traffic-data";
    private static final String GROUP_ID = "traffic-group-v2";
    private static final int SEUIL_VEHICLES  = 100;
    private static final int SEUIL_POLLUTION = 80;
    private static final int SEUIL_NOISE     = 85;

    private final KafkaConsumer<String, String> consumer;
    private final TrafficDataManager            dbManager;
    private volatile boolean                    running = true;

    public KafkaConsumerService() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");

        this.consumer  = new KafkaConsumer<>(props);
        this.dbManager = new TrafficDataManager();

        consumer.subscribe(List.of(TOPIC_NAME));
        System.out.println("[CONSUMER] Abonné au topic : " + TOPIC_NAME);
    }

    public void start() {
        System.out.println("[CONSUMER] En attente de messages...\n");
        try {
            while (running) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(3));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        System.out.printf("[CONSUMER] Message reçu → offset=%d | zone=%s%n",
                record.offset(), record.key());

        TrafficData data = TrafficData.fromKafkaMessage(record.value());

        if (data.getZone() == null) return;

        TrafficDataStore.getInstance().update(data);

        analyzeAndRecommend(data);

        dbManager.save(data);
    }

    private void analyzeAndRecommend(TrafficData data) {
        if (data.getVehicles() > SEUIL_VEHICLES) {
            System.out.printf("[ALERTE] CONGESTION %s (%d veh/min) → Allonger feu vert%n",
                    data.getZone(), data.getVehicles());
        }
        if (data.getPollution() > SEUIL_POLLUTION) {
            System.out.printf("[ALERTE] POLLUTION %s (%d µg/m³) → Réduire trafic%n",
                    data.getZone(), data.getPollution());
        }
        if (data.getNoise() > SEUIL_NOISE) {
            System.out.printf("[ALERTE] BRUIT %s (%d dB) → Alerte sonore%n",
                    data.getZone(), data.getNoise());
        }
        if (data.isAccident()) {
            System.out.printf("[ALERTE] ACCIDENT %s → Déviation%n", data.getZone());
        }
    }

    public void stop() { running = false; }
}

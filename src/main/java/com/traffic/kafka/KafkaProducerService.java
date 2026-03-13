package com.traffic.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.traffic.models.TrafficData;

/**
 * KafkaProducerService — version mise à jour
 * Ajoute la méthode sendRaw() utilisée par TrafficDataManager.
 */
public class KafkaProducerService {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private final KafkaProducer<String, String> producer;
    public void send(TrafficData data) {
    String message = String.format(
        "zone=%s,vehicles=%d,pollution=%d,noise=%d,accident=%b",
        data.getZone(), data.getVehicles(), data.getPollution(),
        data.getNoise(), data.isAccident()
    );
    sendRaw("traffic-data", data.getZone(), message);
}
    public KafkaProducerService() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,     StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                     "1");
        props.put(ProducerConfig.RETRIES_CONFIG,                  3);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * ✅ Méthode utilisée par TrafficDataManager
     * Envoie un message brut (String) vers n'importe quel topic.
     *
     * @param topic   Nom du topic Kafka (ex: "traffic-data")
     * @param key     Clé du message (ex: zone "A1")
     * @param message Message formaté (ex: "zone=A1,vehicles=120,...")
     */
    public void sendRaw(String topic, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("[KAFKA] ✅ Envoyé → topic=%s | key=%s | offset=%d%n",
                    topic, key, metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("[KAFKA] ❌ Erreur envoi : " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void close() {
        producer.flush();
        producer.close();
        System.out.println("[KAFKA] Producer fermé.");
    }
}
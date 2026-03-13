package com.traffic.database;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.traffic.models.FluxVehiculesData;
import com.traffic.models.PollutionData;
import com.traffic.models.TrafficData;
import com.traffic.services.CameraServiceImpl;
import com.traffic.services.ServiceFluxVehicules;

public class TrafficDataManager {

    private static final String TOPIC   = "traffic-data";
    private static final String BROKERS = "localhost:9092";

    // ✅ AJOUT 2 : Producer Kafka
    private final KafkaProducer<String, String> producer;

    public void save(TrafficData data) {
    // ✅ Ignorer les données vides
    if (data.getZone() == null || data.getZone().isEmpty()) {
        System.out.println("[DB] ⚠️ Données ignorées (zone null)");
        return;
    }
    sendToKafka(data.getZone(), data.getVehicles(),
                data.getPollution(), data.getNoise(), data.isAccident());
    }
    // ✅ AJOUT 3 : Constructeur avec initialisation Kafka
    public TrafficDataManager() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "1");
        this.producer = new KafkaProducer<>(props);
    }

    // ✅ AJOUT 4 : Méthode utilitaire d'envoi Kafka
    private void sendToKafka(String zone, int vehicles, int pollution, int noise, boolean accident) {
        String message = String.format(
            "zone=%s,vehicles=%d,pollution=%d,noise=%d,accident=%b",
            zone, vehicles, pollution, noise, accident
        );
        producer.send(new ProducerRecord<>(TOPIC, zone, message));
        System.out.println("[KAFKA] Envoyé → " + message);
    }

    // ─── Votre méthode originale — seules les lignes KAFKA sont nouvelles ────
    public void collectAndStoreData() {
        try {
            TrafficRepository repository = new TrafficRepository();

            // 1) Flux véhicules
            ServiceFluxVehicules serviceFlux = new ServiceFluxVehicules();
            FluxVehiculesData fluxData = serviceFlux.getFluxVehicules("RouteA");
            repository.insertTrafic(fluxData.getRoute(), fluxData.getNombreVehicules());
            sendToKafka(fluxData.getRoute(), fluxData.getNombreVehicules(), 0, 0, false); // ✅ KAFKA

            // 2) Pollution simulée
            PollutionData pollutionData = new PollutionData(
                "ZoneA",
                ThreadLocalRandom.current().nextInt(30, 101)
            );
            repository.insertPollution(pollutionData.getZone(), pollutionData.getPollution());
            sendToKafka(pollutionData.getZone(), 0, pollutionData.getPollution(), 0, false); // ✅ KAFKA

            // 3) Accident
            CameraServiceImpl cameraService = new CameraServiceImpl();
            boolean accident = cameraService.accidentDetecte("CarrefourA");
            repository.insertAccident("CarrefourA", accident);
            if (accident) sendToKafka("CarrefourA", 0, 0, 0, true); // ✅ KAFKA (si accident seulement)

            // 4) Bruit simulé
            int bruit = ThreadLocalRandom.current().nextInt(40, 101);
            repository.insertBruit("ZoneA", bruit);
            sendToKafka("ZoneA", 0, 0, bruit, false); // ✅ KAFKA

            System.out.println("Collecte et stockage terminés.");

        } catch (Exception e) {
            System.out.println("Erreur dans TrafficDataManager : " + e.getMessage());
        }
    }

    // ✅ AJOUT 5 : Fermeture propre du producer
    public void close() {
        producer.flush();
        producer.close();
    }
}
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

    private final KafkaProducer<String, String> producer;

    public void save(TrafficData data) {

    if (data.getZone() == null || data.getZone().isEmpty()) {
        System.out.println("[DB] ⚠️ Données ignorées (zone null)");
        return;
    }
    sendToKafka(data.getZone(), data.getVehicles(),
                data.getPollution(), data.getNoise(), data.isAccident());
    }
    
    public TrafficDataManager() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "1");
        this.producer = new KafkaProducer<>(props);
    }

    
    private void sendToKafka(String zone, int vehicles, int pollution, int noise, boolean accident) {
        String message = String.format(
            "zone=%s,vehicles=%d,pollution=%d,noise=%d,accident=%b",
            zone, vehicles, pollution, noise, accident
        );
        producer.send(new ProducerRecord<>(TOPIC, zone, message));
        System.out.println("[KAFKA] Envoyé → " + message);
    }

    
    public void collectAndStoreData() {
        try {
            TrafficRepository repository = new TrafficRepository();

            // 1) Flux véhicules
            ServiceFluxVehicules serviceFlux = new ServiceFluxVehicules();
            FluxVehiculesData fluxData = serviceFlux.getFluxVehicules("Av_Fal_Ould_Oumeir");
            repository.insertTrafic(fluxData.getRoute(), fluxData.getNombreVehicules());
            sendToKafka(fluxData.getRoute(), fluxData.getNombreVehicules(), 0, 0, false); // ✅ KAFKA

            // 2) Pollution simulée
            PollutionData pollutionData = new PollutionData(
                "Av_Oqba_Ibn_Naafi",
                ThreadLocalRandom.current().nextInt(30, 101)
            );
            repository.insertPollution(pollutionData.getZone(), pollutionData.getPollution());
            sendToKafka(pollutionData.getZone(), 0, pollutionData.getPollution(), 0, false); // ✅ KAFKA

            // 3) Accident
            CameraServiceImpl cameraService = new CameraServiceImpl();
            boolean accident = cameraService.accidentDetecte("Carrefour_Arribat");
            repository.insertAccident("Carrefour_Arribat", accident);
            if (accident) sendToKafka("Carrefour_Arribat", 0, 0, 0, true); // ✅ KAFKA (si accident seulement)

            // 4) Bruit simulé
            int bruit = ThreadLocalRandom.current().nextInt(40, 101);
            repository.insertBruit("Av_Oqba_Ibn_Naafi", bruit);
            sendToKafka("Av_Oqba_Ibn_Naafi", 0, 0, bruit, false); // ✅ KAFKA

            System.out.println("Collecte et stockage terminés.");

        } catch (Exception e) {
            System.out.println("Erreur dans TrafficDataManager : " + e.getMessage());
        }
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}

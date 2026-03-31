package com.traffic;

import com.traffic.api.TrafficHttpServer;
import com.traffic.kafka.KafkaConsumerService;
import com.traffic.kafka.KafkaProducerService;
import com.traffic.models.TrafficData;
import com.traffic.services.ServiceBruitServer;

public class Main {

    public static void main(String[] args) throws Exception {
        String mode = (args.length > 0) ? args[0].toLowerCase() : "both";

        System.out.println("==============================================");
        System.out.println(" Distributed Traffic Management System");
        System.out.println(" Mode: " + mode);
        System.out.println("==============================================\n");

        switch (mode) {
            case "producer":
                runProducer();
                break;
            case "consumer":
                runConsumer();
                break;
            case "collector":
                runCollector();
                break;
            case "server":
                runServer();
                break;
            case "bruitserver":
                runBruitServer();
                break;
            default:
                runBoth();
                break;
        }
    }

    private static void runServer() throws Exception {
        System.out.println("Starting Kafka consumer and HTTP server...\n");

        TrafficHttpServer httpServer = new TrafficHttpServer();
        httpServer.start();

        KafkaConsumerService consumer = new KafkaConsumerService();
        Thread consumerThread = new Thread(consumer::start, "KafkaConsumer");
        consumerThread.setDaemon(false);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[MAIN] Shutdown in progress...");
            consumer.stop();
            httpServer.stop();
        }));

        System.out.println("[SERVER] Dashboard: http://localhost:8080/dashboard");
        System.out.println("[SERVER] API traffic: http://localhost:8080/api/traffic");
        System.out.println("[SERVER] Press Ctrl+C to stop.\n");
    }

    private static void runProducer() throws InterruptedException {
        System.out.println("Starting producer simulation...\n");
        KafkaProducerService producer = new KafkaProducerService();

        TrafficData[] data = {
            new TrafficData("Av_Fal_Ould_Oumeir", 120, 75, 60, false),
            new TrafficData("Av_Ibn_Sina", 45, 90, 55, false),
            new TrafficData("Carrefour_Arribat", 30, 20, 40, false),
            new TrafficData("Av_Oqba_Ibn_Naafi", 80, 60, 95, true),
            new TrafficData("Av_Fal_Ould_Oumeir", 150, 85, 70, false)
        };

        for (TrafficData entry : data) {
            producer.send(entry);
            Thread.sleep(1000);
        }
        producer.close();
        System.out.println("\nProducer messages sent.");
    }

    private static void runConsumer() {
        System.out.println("Starting consumer...\n");
        KafkaConsumerService consumer = new KafkaConsumerService();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop));
        consumer.start();
    }

    private static void runCollector() throws InterruptedException {
        System.out.println("Starting distributed collector...\n");
        KafkaProducerService producer = new KafkaProducerService();
        TrafficDataCollector collector = new TrafficDataCollector(producer);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        while (true) {
            collector.collectAll();
            System.out.println("[COLLECTOR] Next collection in 30 seconds...");
            Thread.sleep(30_000);
        }
    }

    private static void runBruitServer() {
        System.out.println("Starting TCP noise server on port 5000...");
        ServiceBruitServer.main(new String[]{});
    }

    private static void runBoth() throws InterruptedException {
        KafkaConsumerService consumer = new KafkaConsumerService();
        Thread consumerThread = new Thread(consumer::start, "KafkaConsumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        Thread.sleep(2000);
        runProducer();
        consumerThread.join();
    }
}

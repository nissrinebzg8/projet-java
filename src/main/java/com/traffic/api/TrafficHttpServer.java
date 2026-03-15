package com.traffic.api;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * TrafficHttpServer
 * ─────────────────────────────────────────────────────────────────
 * Serveur HTTP léger (intégré à Java, sans dépendance externe)
 * qui expose les données Kafka en JSON pour le dashboard React.
 *
 * Endpoints disponibles :
 *   GET http://localhost:8080/api/traffic   → dernières données par zone
 *   GET http://localhost:8080/api/history   → historique des 50 derniers messages
 *   GET http://localhost:8080/health        → statut du serveur
 *
 * Lancement : java -jar traffic-application-full.jar server
 */
public class TrafficHttpServer {

    private static final int PORT = 8080;
    private final HttpServer server;

    // ─── Constructeur ────────────────────────────────────────────────────────
    public TrafficHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(PORT), 0);

        // ─── Routes ──────────────────────────────────────────────────────────

        // GET /api/traffic → dernières données de toutes les zones
        server.createContext("/api/traffic", exchange -> {
            if (!exchange.getRequestMethod().equals("GET")) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }
            String json = TrafficDataStore.getInstance().getLatestJson();
            sendJson(exchange, 200, json);
        });

        // GET /api/history → historique des messages
        server.createContext("/api/history", exchange -> {
            if (!exchange.getRequestMethod().equals("GET")) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }
            String json = TrafficDataStore.getInstance().getHistoryJson();
            sendJson(exchange, 200, json);
        });

        // GET /health → vérification que le serveur tourne
        server.createContext("/health", exchange -> {
            sendJson(exchange, 200, "{\"status\":\"ok\",\"port\":" + PORT + "}");
        });

        server.setExecutor(null); // thread pool par défaut
    }

    // ─── Méthodes publiques ──────────────────────────────────────────────────

    public void start() {
        server.start();
        System.out.println("[HTTP] Serveur démarré sur http://localhost:" + PORT);
        System.out.println("[HTTP] Endpoints disponibles :");
        System.out.println("[HTTP]   GET http://localhost:" + PORT + "/api/traffic");
        System.out.println("[HTTP]   GET http://localhost:" + PORT + "/api/history");
        System.out.println("[HTTP]   GET http://localhost:" + PORT + "/health");
    }

    public void stop() {
        server.stop(0);
        System.out.println("[HTTP] Serveur arrêté.");
    }

    // ─── Helpers HTTP ────────────────────────────────────────────────────────

    private void sendJson(HttpExchange exchange, int code, String json) throws IOException {
        // Headers CORS pour autoriser le dashboard à faire des requêtes
        exchange.getResponseHeaders().set("Content-Type",   "application/json; charset=UTF-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin",  "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

        byte[] bytes = json.getBytes("UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendResponse(HttpExchange exchange, int code, String message) throws IOException {
        byte[] bytes = message.getBytes("UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
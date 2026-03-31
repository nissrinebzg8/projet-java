package com.traffic.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class TrafficHttpServer {

    private static final int PORT = 8080;
    private final HttpServer server;

    public TrafficHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(PORT), 0);

        server.createContext("/", exchange -> {
            if (!isAllowedMethod(exchange, "GET")) {
                return;
            }
            sendHtml(exchange, 200, loadDashboardHtml());
        });

        server.createContext("/dashboard", exchange -> {
            if (!isAllowedMethod(exchange, "GET")) {
                return;
            }
            sendHtml(exchange, 200, loadDashboardHtml());
        });

        server.createContext("/api/traffic", exchange -> {
            if (!isAllowedMethod(exchange, "GET")) {
                return;
            }
            sendJson(exchange, 200, TrafficDataStore.getInstance().getLatestJson());
        });

        server.createContext("/api/history", exchange -> {
            if (!isAllowedMethod(exchange, "GET")) {
                return;
            }
            sendJson(exchange, 200, TrafficDataStore.getInstance().getHistoryJson());
        });

        server.createContext("/health", exchange -> sendJson(exchange, 200, "{\"status\":\"ok\",\"port\":" + PORT + "}"));

        server.setExecutor(null);
    }

    public void start() {
        server.start();
        System.out.println("[HTTP] Server started on http://localhost:" + PORT);
        System.out.println("[HTTP] Dashboard: http://localhost:" + PORT + "/dashboard");
        System.out.println("[HTTP] API traffic: http://localhost:" + PORT + "/api/traffic");
        System.out.println("[HTTP] API history: http://localhost:" + PORT + "/api/history");
        System.out.println("[HTTP] Health: http://localhost:" + PORT + "/health");
    }

    public void stop() {
        server.stop(0);
        System.out.println("[HTTP] Server stopped.");
    }

    private boolean isAllowedMethod(HttpExchange exchange, String expected) throws IOException {
        String method = exchange.getRequestMethod();
        if ("OPTIONS".equalsIgnoreCase(method)) {
            addCorsHeaders(exchange);
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return false;
        }
        if (!expected.equalsIgnoreCase(method)) {
            sendText(exchange, 405, "Method Not Allowed", "text/plain; charset=UTF-8");
            return false;
        }
        return true;
    }

    private void sendJson(HttpExchange exchange, int code, String json) throws IOException {
        sendText(exchange, code, json, "application/json; charset=UTF-8");
    }

    private void sendHtml(HttpExchange exchange, int code, String html) throws IOException {
        sendText(exchange, code, html, "text/html; charset=UTF-8");
    }

    private void sendText(HttpExchange exchange, int code, String body, String contentType) throws IOException {
        addCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private String loadDashboardHtml() throws IOException {
        try (InputStream input = TrafficHttpServer.class.getResourceAsStream("/com/traffic/Dashboard.html")) {
            if (input == null) {
                return "<html><body><h1>Dashboard not found</h1></body></html>";
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}

package com.traffic.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TrafficRepository {

    // Insere des donnees dans la table trafic
    public void insertTrafic(String route, int nombreVehicules) {
        String sql = "INSERT INTO trafic(route, nombre_vehicules) VALUES (?, ?)";
        executeUpdate(sql, route, nombreVehicules);
    }

    // Insere des donnees dans la table pollution
    public void insertPollution(String zone, int niveauPollution) {
        String sql = "INSERT INTO pollution(zone, niveau_pollution) VALUES (?, ?)";
        executeUpdate(sql, zone, niveauPollution);
    }

    // Insere des donnees dans la table accident
    public void insertAccident(String zone, boolean accidentDetecte) {
        String sql = "INSERT INTO accident(zone, accident_detecte) VALUES (?, ?)";
        executeUpdate(sql, zone, accidentDetecte);
    }

    // Insere des donnees dans la table bruit
    public void insertBruit(String zone, int niveauBruit) {
        String sql = "INSERT INTO bruit(zone, niveau_bruit) VALUES (?, ?)";
        executeUpdate(sql, zone, niveauBruit);
    }

    // Insere des donnees dans la table alertes
    public void insertAlerte(String typeAlerte, String message) {
        String sql = "INSERT INTO alertes(type_alerte, message) VALUES (?, ?)";
        executeUpdate(sql, typeAlerte, message);
    }

    // Methode utilitaire pour executer les insertions JDBC
    private void executeUpdate(String sql, Object... params) {
        try (Connection connection = DatabaseConnection.getConnection()) {
            if (connection == null) {
                System.out.println("Connexion MySQL indisponible.");
                return;
            }

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Erreur lors de l'insertion: " + e.getMessage());
        }
    }
}

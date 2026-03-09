package com.traffic.tests;

import java.sql.Connection;
import java.sql.SQLException;

import com.traffic.database.DatabaseConnection;

public class TestDatabaseConnection {
    public static void main(String[] args) {
        // Test de la connexion a la base MySQL
        Connection connection = DatabaseConnection.getConnection();

        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    System.out.println("Connexion réussie");
                } else {
                    System.out.println("Erreur: la connexion est fermée");
                }
                // Ferme la connexion apres le test
                connection.close();
            } catch (SQLException e) {
                System.out.println("Erreur lors du test de connexion: " + e.getMessage());
            }
        } else {
            System.out.println("Erreur de connexion");
        }
    }
}

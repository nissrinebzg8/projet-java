package com.traffic.analysis;

public class AnalyseSeuils {

    // Analyse le flux de vehicules et retourne un message simple
    public static String analyserFlux(int flux) {
        if (flux > 100) {
            return "Congestion détectée";
        }
        return "Trafic normal";
    }

    // Analyse le niveau de pollution et retourne un message simple
    public static String analyserPollution(int pollution) {
        if (pollution > 70) {
            return "Alerte pollution";
        }
        return "Pollution normale";
    }

    // Analyse la presence d'un accident
    public static String analyserAccident(boolean accident) {
        if (accident) {
            return "Déviation recommandée";
        }
        return "Aucun accident";
    }

    // Analyse le niveau de bruit et retourne un message simple
    public static String analyserBruit(int bruit) {
        if (bruit > 80) {
            return "Alerte bruit";
        }
        return "Bruit normal";
    }
}

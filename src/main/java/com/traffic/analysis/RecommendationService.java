package com.traffic.analysis;

public class RecommendationService {

    // Recommandation liee au flux de vehicules
    public static String recommanderPourFlux(int flux) {
        if (flux > 100) {
            return "Allonger le feu vert";
        }
        return "Aucune action sur les feux";
    }

    // Recommandation liee au niveau de pollution
    public static String recommanderPourPollution(int pollution) {
        if (pollution > 70) {
            return "Réduire le trafic dans la zone";
        }
        return "Aucune action environnementale";
    }

    // Recommandation liee a un accident
    public static String recommanderPourAccident(boolean accident) {
        if (accident) {
            return "Dévier le trafic vers une route secondaire";
        }
        return "Pas de déviation nécessaire";
    }

    // Recommandation liee au bruit
    public static String recommanderPourBruit(int bruit) {
        if (bruit > 80) {
            return "Surveiller la zone bruyante";
        }
        return "Bruit acceptable";
    }
}

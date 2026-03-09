package com.traffic.tests;

import com.traffic.analysis.RecommendationService;

public class TestRecommendationService {
    public static void main(String[] args) {
        // Valeurs d'exemple pour tester les recommandations
        int flux = 130;
        int pollution = 80;
        boolean accident = true;
        int bruit = 85;

        // Appels des methodes puis affichage des resultats
        System.out.println("Flux: " + RecommendationService.recommanderPourFlux(flux));
        System.out.println("Pollution: " + RecommendationService.recommanderPourPollution(pollution));
        System.out.println("Accident: " + RecommendationService.recommanderPourAccident(accident));
        System.out.println("Bruit: " + RecommendationService.recommanderPourBruit(bruit));
    }
}

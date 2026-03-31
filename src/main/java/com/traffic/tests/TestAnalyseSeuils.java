package com.traffic.tests;

import com.traffic.analysis.AnalyseSeuils;

public class TestAnalyseSeuils {
    public static void main(String[] args) {
        // Exemples de valeurs a analyser
        int flux = 120;
        int pollution = 75;
        boolean accident = true;
        int bruit = 85;

        // Appels des methodes d'analyse et affichage des resultats
        System.out.println("Flux: " + AnalyseSeuils.analyserFlux(flux));
        System.out.println("Pollution: " + AnalyseSeuils.analyserPollution(pollution));
        System.out.println("Accident: " + AnalyseSeuils.analyserAccident(accident));
        System.out.println("Bruit: " + AnalyseSeuils.analyserBruit(bruit));
    }
}

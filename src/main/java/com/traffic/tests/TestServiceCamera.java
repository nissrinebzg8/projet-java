package com.traffic.tests;

import java.rmi.RemoteException;

import com.traffic.services.CameraServiceImpl;

public class TestServiceCamera {
    public static void main(String[] args) {
        try {
            // Creation d'une instance locale du service camera
            CameraServiceImpl cameraService = new CameraServiceImpl();

            // Test de detection sur la zone CarrefourA
            boolean accident = cameraService.accidentDetecte("Carrefour_Arribat");

            // Affiche le resultat dans la console
            System.out.println("Accident detecte sur CarrefourA : " + accident);
        } catch (RemoteException e) {
            // Gestion simple d'une erreur RMI
            System.out.println("Erreur RMI: " + e.getMessage());
        }
    }
}

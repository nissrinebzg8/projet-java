package com.traffic.services;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ThreadLocalRandom;

// Implementation concrete du service RMI CameraService
public class CameraServiceImpl extends UnicastRemoteObject implements CameraService {

    // Constructeur obligatoire pour un objet RMI exporte
    public CameraServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public boolean accidentDetecte(String zone) throws RemoteException {
        // Si la zone est nulle, on prend une valeur par defaut
        if (zone == null) {
            zone = "CarrefourA";
        }

        // Simulation: retourne aleatoirement true ou false
        return ThreadLocalRandom.current().nextBoolean();
    }
}

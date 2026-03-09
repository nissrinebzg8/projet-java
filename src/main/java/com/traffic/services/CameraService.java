package com.traffic.services;

import java.rmi.Remote;
import java.rmi.RemoteException;

// Interface RMI: declare les methodes accessibles a distance
public interface CameraService extends Remote {

    // Detecte s'il y a un accident dans une zone
    boolean accidentDetecte(String zone) throws RemoteException;
}

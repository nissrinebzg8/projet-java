package com.traffic.models;

public class FluxVehiculesData {
    // Nom de la route (ex: RouteA)
    private String route;
    // Nombre de vehicules detectes sur la route
    private int nombreVehicules;

    // Constructeur vide
    public FluxVehiculesData() {
    }

    // Constructeur avec valeurs initiales
    public FluxVehiculesData(String route, int nombreVehicules) {
        this.route = route;
        this.nombreVehicules = nombreVehicules;
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }

    public int getNombreVehicules() {
        return nombreVehicules;
    }

    public void setNombreVehicules(int nombreVehicules) {
        this.nombreVehicules = nombreVehicules;
    }

    @Override
    public String toString() {
        return "FluxVehiculesData{" +
                "route='" + route + '\'' +
                ", nombreVehicules=" + nombreVehicules +
                '}';
    }
}

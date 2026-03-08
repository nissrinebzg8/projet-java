package com.traffic.models;

public class FluxVehiculesData {
    private String route;
    private int nombreVehicules;

    public FluxVehiculesData() {
    }

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
}
package com.traffic.services;

import java.util.concurrent.ThreadLocalRandom;

import com.traffic.models.FluxVehiculesData;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public class ServiceFluxVehicules {

    @WebMethod
    public FluxVehiculesData getFluxVehicules(String route) {
        if (route == null || route.isBlank()) {
            route = "RouteA";
        }

        int nombreVehicules = ThreadLocalRandom.current().nextInt(0, 151);
        return new FluxVehiculesData(route, nombreVehicules);
    }
}
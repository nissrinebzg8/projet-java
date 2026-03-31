package com.traffic.services;

import java.util.concurrent.ThreadLocalRandom;

import com.traffic.models.PollutionData;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

@Path("/pollution")
@Produces(MediaType.APPLICATION_JSON)
public class ServicePollution {

    @GET
    public PollutionData getPollution(@QueryParam("zone") String zone) {
        if (zone == null || zone.isBlank()) {
            zone = "Av_Fal_Ould_Oumeir";
        }

        int pollution = ThreadLocalRandom.current().nextInt(30, 101);
        return new PollutionData(zone, pollution);
    }
}

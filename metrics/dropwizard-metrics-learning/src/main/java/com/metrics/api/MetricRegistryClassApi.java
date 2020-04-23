package com.metrics.api;

import com.codahale.metrics.*;

public class MetricRegistryClassApi {

    public static void main(String[] args) {
        final MetricRegistry metrics = new MetricRegistry();

        Counter counter = metrics.counter("counter-name");
        Meter meter = metrics.meter("meter-name");
        Histogram histogram = metrics.histogram("histogram-name");
        Timer timer = metrics.timer("timer-name");

        metrics.gauge("",null);
        metrics.counter("",null);
        metrics.meter("",null);
        metrics.histogram("",null);
        metrics.timer("",null);

        metrics.addListener(null); //new MetricRegistryListener() {}

        metrics.register("",null);
        metrics.registerAll(null);
        metrics.registerAll("",null);

        metrics.remove("");
        metrics.removeListener(null);
        metrics.removeMatching(null);

        metrics.getCounters();
        metrics.getGauges();
        metrics.getMeters();
        metrics.getHistograms();
        metrics.getTimers();

        metrics.getNames();

        String name = MetricRegistry.name(MetricRegistryClassApi.class, "jobs", "size");
    }
}

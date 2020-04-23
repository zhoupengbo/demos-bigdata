package com.metrics.api;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class CounterClassApi {

    public static void main(String[] args) {
        final MetricRegistry metrics = new MetricRegistry();
        Counter counter = metrics.counter(MetricRegistry.name(CounterClassApi.class, "counter-name"));

        counter.dec();
        counter.dec(10L);
    }
}

package com.metrics.use;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class GaugeTest {
    public static Queue<String> q = new LinkedList<String>();

    public static void main(String[] args) throws InterruptedException {
        MetricRegistry registry = new MetricRegistry();
        registry.register(MetricRegistry.name(GaugeTest.class, "queue", "size"),
                new Gauge<Integer>() {
                    public Integer getValue() {
                        return q.size();
                    }
                });

        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
        reporter.start(1, TimeUnit.SECONDS);

        while(true){
            Thread.sleep(100);
            q.add("Job-xxx");
        }


    }
}

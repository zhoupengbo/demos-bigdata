package com.metrics.use;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricRegistry;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MeterTest {

    public static Random random = new Random();

    public static void request(Meter meter){
        meter.mark();
    }

    public static void request(Meter meter, int n){
        System.out.println("n:"+n);
        while(n > 0){
            request(meter);
            n--;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MetricRegistry registry = new MetricRegistry();
        Meter meterTps = registry.meter(MetricRegistry.name(MeterTest.class,"request","tps"));

        meterTps.mark();
        meterTps.mark(10L);
        meterTps.getCount();
        meterTps.getMeanRate();
        meterTps.getOneMinuteRate();
        meterTps.getFiveMinuteRate();
        meterTps.getFifteenMinuteRate();

        MetricAttribute m1Rate = MetricAttribute.M1_RATE;
        ConsoleReporter reporter = ConsoleReporter
                .forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .disabledMetricAttributes(new HashSet<MetricAttribute>(){})
                .build();
        reporter.start(3, TimeUnit.SECONDS);


        while(true){
            request(meterTps,random.nextInt(5));
            Thread.sleep(1000);
        }

    }
}

package com.metrics.use;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.Queue;

public class QueueManager {
    private final Queue queue;

    public QueueManager(MetricRegistry metrics, String name) {
        this.queue = new LinkedList<String>();
        metrics.register(MetricRegistry.name(QueueManager.class, name, "size"),
                new Gauge<Integer>() {
                    public Integer getValue() {
                        return queue.size();
                    }
                });
    }
}

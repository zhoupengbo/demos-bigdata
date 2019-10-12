package com.zpb.demos.api.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;

/**
 * 获取生产商维护的全套内部度量。
 * successful-authentication-total=0.0
 * failed-authentication-total=0.0
 * io-wait-ratio=0.0
 * outgoing-byte-rate=0.0
 * record-queue-time-avg=0.0
 * incoming-byte-rate=0.0
 * request-latency-max=-Infinity
 * request-rate=0.0
 * commit-id=aaa7af6d4a11b29d
 * connection-count=0.0
 * io-wait-time-ns-avg=0.0
 * buffer-exhausted-rate=0.0
 * request-size-avg=0.0
 * version=1.0.0
 * request-size-max=-Infinity
 * incoming-byte-total=0.0
 * bufferpool-wait-ratio=0.0
 * waiting-threads=0.0
 * requests-in-flight=0.0
 * connection-close-total=0.0
 * batch-size-max=-Infinity
 * io-time-ns-avg=0.0
 * produce-throttle-time-avg=0.0
 * bufferpool-wait-time-total=0.0
 * io-waittime-total=0.0
 * response-rate=0.0
 * successful-authentication-rate=0.0
 * record-queue-time-max=-Infinity
 * produce-throttle-time-max=-Infinity
 * record-retry-total=0.0
 * connection-creation-rate=0.0
 * record-size-avg=0.0
 * batch-split-rate=0.0
 * record-send-rate=0.0
 * select-total=0.0
 * network-io-total=0.0
 * buffer-total-bytes=3.3554432E7
 * buffer-exhausted-total=0.0
 * failed-authentication-rate=0.0
 * compression-rate-avg=0.0
 * batch-split-total=0.0
 * select-rate=0.0
 * connection-close-rate=0.0
 * connection-creation-total=0.0
 * metadata-age=0.09
 * iotime-total=0.0
 * record-size-max=-Infinity
 * record-error-rate=0.0
 * record-send-total=0.0
 * count=61.0
 * buffer-available-bytes=3.3554432E7
 * records-per-request-avg=0.0
 * network-io-rate=0.0
 * record-error-total=0.0
 * outgoing-byte-total=0.0
 * batch-size-avg=0.0
 * response-total=0.0
 * request-total=0.0
 * record-retry-rate=0.0
 * io-ratio=0.0
 * request-latency-avg=0.0
 */
public class MetricsAPI {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all"); // 此配置是 Producer 在确认一个请求发送完成之前需要收到的反馈信息的数量。
        props.put("retries", 0); // 若设置大于0的值，则客户端会将发送失败的记录重新发送，允许 retries 并且没有设置max.in.flight.requests.per.connection 为1时，记录的顺序可能会被改变。
        props.put("batch.size", 16384); // 配置控制一个批次的默认大小（以字节为单位）小的 batch.size 将减少批处理，并且可能会降低吞吐量(如果 batch.size = 0的话将完全禁用批处理
        props.put("linger.ms", 1); // 如果设置linger.ms=5 ，则发送的请求会减少并降低部分负载，但同时会增加5毫秒的延迟。
        props.put("buffer.memory", 33554432);  // Producer 用来缓冲等待被发送到服务器的记录的总字节数。一个分区一个缓冲区。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Map<MetricName, ? extends Metric> metrics = producer.metrics();
        metrics.forEach((k,v)->{
            String metricsName = k.name();
            String metricsValue = v.metricValue().toString();
            System.out.println(metricsName+"="+metricsValue);
        });
        producer.close();
    }

}

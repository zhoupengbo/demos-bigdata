package com.zpb.demos.api.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;

/**
 * 将记录发布到Kafka群集的Kafka客户端。
 * producer是线程安全的，跨线程共享单个producer实例通常比拥有多个实例更快。
 * 下面是一个简单的示例，使用producer发送包含序列号的字符串作为键/值对的记录。
 * http://kafka.apachecn.org/10/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 */
public class CommonMode {

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
        for (int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<String, String>("ztest", Integer.toString(i)));  // 异步
            producer.send(new ProducerRecord<String, String>("ztest", Integer.toString(i), Integer.toString(i)));  // 异步
            producer.send(new ProducerRecord<String, String>("ztest", 0, Integer.toString(i), Integer.toString(i)));  // 异步
        }
        Map<MetricName, ? extends Metric> metrics = producer.metrics();
        metrics.forEach((k,v)->{
            System.out.println(k);
//            System.out.println(v);
        });
        producer.close();
    }

}

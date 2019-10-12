package com.zpb.demos.api.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 等幂生产者
 * 等幂生产者将kafka的交付语义从至少一次增强到了完全一次。特别是生产者的重试将不再引入重复。
 * 要启用等幂，enable.idempotence配置必须设置为true。如果设置，retries配置将默认为integer.max_value，acks配置将默认为all。
 * 对于等幂生产者没有api更改，因此无需修改现有应用程序即可利用此功能。
 * 为了利用等幂生产者，必须避免应用程序级的重新发送，因为它们不能消除重复。因此，如果应用程序启用幂等，建议不设置retries config，
 * 因为它将默认为integer.max_value。此外，如果发送（producerrecord）返回一个错误，即使是无限次重试（例如，如果消息在发送前在缓冲区中过期），
 * 则建议关闭生产者并检查最后生成的消息的内容，以确保它不重复。最后，生产者只能保证在单个会话中发送的消息具有幂等性。
 * http://kafka.apachecn.org/10/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 */
public class IdempotentMode {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.idempotence", true);
        props.put("batch.size", 16384); // 配置控制一个批次的默认大小（以字节为单位）小的 batch.size 将减少批处理，并且可能会降低吞吐量(如果 batch.size = 0的话将完全禁用批处理
        props.put("linger.ms", 1); // 如果设置linger.ms=5 ，则发送的请求会减少并降低部分负载，但同时会增加5毫秒的延迟。
        props.put("buffer.memory", 33554432);  // Producer 用来缓冲等待被发送到服务器的记录的总字节数。一个分区一个缓冲区。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<String, String>("ztest", Integer.toString(i), Integer.toString(i)));  // 异步
        }

        producer.close();
    }

}

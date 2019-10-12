package com.zpb.demos.api.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * 自动提交偏移量
 * 使用后不关闭耗电元件会泄漏这些连接。使用者不是线程安全的。有关详细信息，请参见多线程处理。
 * 这个例子演示了依赖于自动偏移提交的kafka消费者api的简单用法。
 */
public class AutomaticOffsetMode {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true"); // 设置enable.auto.commit意味着以config auto.commit.interval.ms控制的频率自动提交偏移量。
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.poll.interval.ms", "300000"); // 默认300000  调用poll（）之间的最大延迟。
        props.put("max.poll.records", "500"); // 默认500  一次调用poll（）返回的最大记录数。
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ztest", "test"));  // 订阅主题
        while (true) {
            System.out.println(new Date());
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}

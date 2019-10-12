package com.zpb.demos.api.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 偏移控制偏移量
 * 用户也可以控制何时应将记录视为已消耗，从而提交其偏移量，而不是依赖于使用者定期提交已消耗的偏移量。
 * 当消息的消耗与某些处理逻辑耦合时，这非常有用，因此在完成处理之前，不应将消息视为已消耗。
 */
public class ManualOffsetMode {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ztest", "test"));
//        demo1(consumer);
        demo2(consumer);

    }

    /**
     * 示例使用commitsync将所有接收到的记录标记为已提交。
     */
    public static void demo1(KafkaConsumer<String, String> consumer){

        final int minBatchSize = 3;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                System.out.println("coming...");
                // insertIntoDb(buffer); 插入数据库中
                consumer.commitSync(); // 手动提交偏移量
                buffer.clear();
            }
        }
    }

    /**
     * 在下面的示例中，我们在处理完每个分区中的记录后通过显式指定偏移量。
     * 注意：提交的偏移量应该始终是应用程序将读取的下一条消息的偏移量。因此，在调用commitsync（offset）时，应该在最后处理的消息的偏移量中添加一个。
     */
    public static void demo2(KafkaConsumer<String, String> consumer){
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}

package com.zpb.demos.api.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 事务生产者
 * 事务生产者允许应用程序将消息发送到多个分区（和主题！）原子的。
 * 要使用事务生产者和助理API，必须设置transactional.id配置属性。如果设置了transactional.id，则会自动启用等幂，同时生产者会配置等幂依赖于哪个。
 * 此外，事务中包含的主题应配置为具有持久性。特别是，replication.factor应至少为3，这些主题的min.insync.replicas应设置为2。
 * 最后，为了从端到端实现事务性保证，还必须将使用者配置为只读提交的消息。transactional.id的目的是在单个生产者实例的多个会话之间启用事务恢复。
 * 它通常从分区的、有状态的应用程序中的shard标识符派生。因此，对于在分区应用程序中运行的每个生产者实例，它应该是唯一的。
 * 所有新的事务性api都被阻塞，并在失败时抛出异常。下面的示例说明了如何使用新的api。它与上面的示例类似，只是所有100条消息都是单个事务的一部分。
 */
public class TransactionalMode {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", "my-transactional-id");
        props.put("replication.factor", 5); // 至少为3
        props.put("min.insync.replicas", 2);
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++){
                producer.send(new ProducerRecord<>("ztest", Integer.toString(i), Integer.toString(i)));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }
}

package fr.freetech4u.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PartitionConsumer {

    @KafkaListener(topics = "partition-topic", groupId = "partition-group")
    public void listen(ConsumerRecord<String , String> record,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Received message: key={}, value={}, partition={}, offset={}",
                record.key(), record.value(), partition, record.offset());
    }

    @KafkaListener(topics = "partition-topic", groupId = "partition-group")
    public void listen2(ConsumerRecord<String , String> record,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Received2 message: key={}, value={}, partition={}, offset={}",
                record.key(), record.value(), partition, record.offset());
    }
}

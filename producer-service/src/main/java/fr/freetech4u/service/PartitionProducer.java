package fr.freetech4u.service;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class PartitionProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicInteger messageCount = new AtomicInteger(0);


    public PartitionProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String value) {
        int messageId = messageCount.incrementAndGet();
        String messageWithId = "Message-" + messageId + ": " + value;
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("partition-topic", key, messageWithId);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent Message: " + messageWithId
                + "With key " + key
                + "to partition " + result.getRecordMetadata().partition());
            } else {
                System.err.println("Failed to send message: " + ex.getMessage());
            }
        });
    }

    public void sendBulkMessages() {
        String [] keys = {"key-A", "key-B", "key-C", "key-D", "key-E", "key-F", "key-G", "key-H"};
        for (int i = 0; i < 100; i++) {
            String key = keys[i % keys.length];
            String value = "Value for " + key + " - " + i;
            sendMessage(key, value);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

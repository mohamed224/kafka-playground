package fr.freetech4u.controller;

import fr.freetech4u.service.PartitionProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor
public class ProducerController {

    private final PartitionProducer partitionProducer;

    @PostMapping("/send-bulk")
    public String sendBulkMessages() {
        partitionProducer.sendBulkMessages();
        return "Bulk messages sent!";
    }

    @PostMapping("/send")
    public String sendMessage(String key, String value) {
        partitionProducer.sendMessage(key, value);
        return "Message sent with key: " + key;
    }
}

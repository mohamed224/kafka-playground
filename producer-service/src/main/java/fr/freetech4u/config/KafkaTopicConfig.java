package fr.freetech4u.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopicConfig {


    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("partition-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }
}

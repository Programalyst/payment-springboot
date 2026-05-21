package com.demo.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic myExpiredTopic(AppKafkaProperties properties) {

        return TopicBuilder.name(properties.getPaymentTopic())
                .partitions(3)
                .replicas(1) // Adjust based on broker count
                .config("retention.ms", properties.getPaymentTopicRetentionMs())
                .build();
    }
}

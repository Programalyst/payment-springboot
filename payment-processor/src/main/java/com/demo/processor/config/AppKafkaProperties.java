package com.demo.processor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {
    private String paymentTopic;
    private String paymentTopicRetentionMs;

    // Getters and Setters
    public String getPaymentTopic() { return paymentTopic; }
    public void setPaymentTopic(String paymentTopic) { this.paymentTopic = paymentTopic; }
    public String getPaymentTopicRetentionMs() { return paymentTopicRetentionMs; }
    public void setPaymentTopicRetentionMs(String paymentTopicRetentionMs) { this.paymentTopicRetentionMs = paymentTopicRetentionMs; }
}
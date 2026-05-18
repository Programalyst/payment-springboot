package com.demo.processor.config;

import com.demo.processor.dto.TransactionProcessedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

// must explicitly tell Spring how to serialize TransactionProcessedEvent object with a Configuration Class
// equivalent of the Program.cs / Dependency Injection configuration layer in .NET Core
@Configuration
public class KafkaProducerConfig {
    // Kafka only stores raw arrays of bytes (byte[])
    // Transaction is a rich Java class structure residing in memory.
    // Before sending it across the network to the Docker container, that structure must be flattened into bytes (serialized)

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // This defines a factory that creates KafkaProducer objects
    @Bean
    public ProducerFactory<String, TransactionProcessedEvent> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Use Spring's built-in Kafka JSON Serializer for the value
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    // This explicitly creates the bean PaymentController is looking for
    // whenever KafkaTemplate is requested, it will create a new KafkaProducer object
    // KafkaTemplate is a wrapper around KafkaProducer that provides a higher-level API for sending and receiving messages
    // hence why it is produced by the producerFactory()
    @Bean
    public KafkaTemplate<String, TransactionProcessedEvent> kafkaTemplate() { // <> operator infers type from here
        // <> diamond operator infers type of producerFactory() <String, TransactionProcessedEvent>
        return new KafkaTemplate<>(producerFactory()); // without the <> operator, there is no type safety
    }
}

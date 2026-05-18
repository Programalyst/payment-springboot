package com.demo.processor.service;

import com.demo.processor.dto.TransactionProcessedEvent;
import com.demo.processor.model.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ParallelBatchProcessor {

    // MOCK DATABASE: Maps CardID -> [Balance, Version]
    private final ConcurrentHashMap<String, Object[]> mockDatabase = new ConcurrentHashMap<>();

    @Value("${app.kafka.payment-topic}")
    private String KAFKA_TOPIC; // Injects "payment-topic"
    // Publish transaction messages directly to Kafka whenever a payment request is processed
    // KafkaTemplate is like HttpClient in .NET or Axios in JavaScript, but for Kafka
    private final KafkaTemplate<String, TransactionProcessedEvent> kafkaTemplate;

    public ParallelBatchProcessor(KafkaTemplate<String, TransactionProcessedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        // Seed dummy data: Card "123" has $5000.00 balance, Version 1
        mockDatabase.put("123", new Object[]{5000.00, 1});
    }

    public void processCardBatch(String cardId, List<Transaction> batch) {
        // 1. READ PHASE: Fetch state from "DB"
        Object[] dbRecord = mockDatabase.get(cardId);
        double currentBalance = (double) dbRecord[0];
        int initialVersion = (int) dbRecord[1];

        // 2. IN-MEMORY BATCH PROCESSING PHASE
        double runningBalance = currentBalance;
        for (Transaction tx : batch) {
            // Singleton Validator logic happens here implicitly via Spring beans
            if (runningBalance >= tx.getAmount()) {
                runningBalance -= tx.getAmount();
                tx.setStatus("APPROVED");
            } else {
                tx.setStatus("DECLINED_INSUFFICIENT_FUNDS");
            }
        }

        // Capture the final calculated balance in an effectively final variable for the lambda
        final double finalBalance = runningBalance;

        // 3. WRITE/VALIDATE PHASE (Optimistic Concurrency Control)
        Object[] updatedRecord = mockDatabase.computeIfPresent(cardId, (key, currentDbRecord) -> {
            int currentVersion = (int) currentDbRecord[1];

            // OCC Check: Has another thread updated this card while we were processing?
            if (currentVersion == initialVersion) {
                // Success! Update balance and increment version
                return new Object[]{finalBalance, currentVersion + 1};
            }
            // Fail! Return unchanged record to signal failure
            return currentDbRecord;
        });

        // Check if write succeeded by verifying the version incremented
        boolean success = updatedRecord != null && ((int) updatedRecord[1] == initialVersion + 1);

        if (success) {
            System.out.println("OCC Success for Card " + cardId + ". New Balance: " + finalBalance);
            // STREAM TO KAFKA: Loop through the finalized batch and publish events
            for (Transaction tx : batch) {
                TransactionProcessedEvent event = new TransactionProcessedEvent(
                        tx.getTransactionId(),
                        tx.getCardId(),
                        tx.getAmount(),
                        tx.getStatus()
                );
                // Pass the cardId as the Kafka MESSAGE KEY (Second Parameter): Partition by cardId
                // guarantees every single transaction for a specific card goes to the exact same Kafka partition
                // guarantees ordered processing
                kafkaTemplate.send(KAFKA_TOPIC, tx.getCardId(), event);
            }

        } else {
            System.out.println("OCC Conflict Detected for Card " + cardId + "! Retrying batch...");
            processCardBatch(cardId, batch);
        }
    }
}
package com.demo.processor.dto;

// use immutable Java Record for DTOs
// DTOs are used for ingress/egress of data between components
public record TransactionProcessedEvent(
        String transactionId,
        String cardId,
        double amount,
        String status
) {}
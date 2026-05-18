package com.demo.processor.dto;

/**
 * A Java Record is perfect for DTOs.
 * It is immutable by default and contains 0 boilerplate.
 */
public record TransactionResponse (
    String id,
    String status,
    double amount,
    String processedBy){}


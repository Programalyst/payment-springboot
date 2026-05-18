package com.demo.processor.dto;

public record PaymentRequest(
        String cardId,
        double amount) {}
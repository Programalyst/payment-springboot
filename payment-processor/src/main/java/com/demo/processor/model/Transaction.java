package com.demo.processor.model;

public class Transaction{
    private final String transactionId;
    private final String cardId;
    private final double amount;
    private String status; // Mutable field

    // Constructor
    public Transaction(String transactionId, String cardId, double amount) {
        this.transactionId = transactionId;
        this.cardId = cardId;
        this.amount = amount;
        this.status = "PENDING"; // Default status
    }

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public String getCardId() { return cardId; }
    public double getAmount() { return amount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

}

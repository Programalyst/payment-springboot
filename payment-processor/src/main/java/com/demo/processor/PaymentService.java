package com.demo.processor;

import com.demo.processor.dto.TransactionResponse;
import org.springframework.stereotype.Service;

@Service // is a singleton by default
public class PaymentService {

    public TransactionResponse getPaymentStatus(String transactionId) {
        return new TransactionResponse(
        "123456",
        "APPROVED",
        99.0, Thread.currentThread().toString()
        );
    }

    public TransactionResponse processPayment(String transactionId, double amount) {
        // Log the current thread name to see Virtual Thread info in the console
        System.out.println("Processing on: " + Thread.currentThread());
        return new TransactionResponse(
            "654321",
            "APPROVED",
            amount,
            Thread.currentThread().toString()
        );
    }
}

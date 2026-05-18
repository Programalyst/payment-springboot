package com.demo.processor;

import com.demo.processor.dto.PaymentRequest;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Controller;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import com.demo.processor.dto.TransactionResponse;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
public class PaymentController {

    @Value("${app.kafka.payment-topic}")
    private String paymentTopic; // Injects "payment-topic"

    private final PaymentService paymentService;

    // Publish transaction messages directly to Kafka whenever a payment request is received
    // KafkaTemplate is like HttpClient in .NET or Axios in JavaScript, but for Kafka
    private final KafkaTemplate<String, TransactionResponse> kafkaTemplate;

    // must explicitly tell Spring how to serialize TransactionResponse Record object with a Configuration Class
    public PaymentController(PaymentService paymentService, KafkaTemplate<String, TransactionResponse> kafkaTemplate) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/{id}")
    public TransactionResponse getPaymentStatus(@PathVariable String id) {
        return paymentService.getPaymentStatus(id);
    }

    @PostMapping // Empty means it maps exactly to "/api/v1/payments"
    public String processPayment(@RequestBody PaymentRequest paymentRequest) {
        String transactionId = UUID.randomUUID().toString();
        TransactionResponse response = paymentService.processPayment(transactionId, paymentRequest.amount());

        // Pass the transaction ID as the Kafka MESSAGE KEY (Second Parameter)
        // Any event tied to a transaction (PAYMENT_INITIATED, SETTLED, or REFUNDED must use the same Kafka key
        // to ensure the events are hashed to the same Kafka partition
        kafkaTemplate.send(paymentTopic, transactionId, response);
        return "Transaction queued: " + transactionId;
    }
}

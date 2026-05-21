package com.demo.processor;

import com.demo.processor.dto.PaymentRequest;
import com.demo.processor.model.Transaction;
import com.demo.processor.service.BatchProcessingManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
public class PaymentController {

    private BatchProcessingManager batchManager;

    public PaymentController() {
    }

    @PostMapping // Empty means it maps exactly to "/api/v1/payments"
    public ResponseEntity<String> processPayment(@RequestBody PaymentRequest request) {
        String transactionId = UUID.randomUUID().toString();

        // Asynchronously queue the transaction for batching by Card ID
        Transaction internalTx = new Transaction(
                transactionId,
                request.cardId(),
                request.amount()
        );
        batchManager.queueTransaction(internalTx);

        return ResponseEntity.accepted().body("Transaction " + transactionId + " accepted for batch processing");
    }
}
